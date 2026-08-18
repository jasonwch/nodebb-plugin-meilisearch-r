'use strict';

const winston = nodebb.require('winston');
const settings = nodebb.require('./src/meta/settings');
const Topics = nodebb.require('./src/topics');
const { drainPending } = require('./pending-queue');
const {
	EMBEDDER_NAME, buildEmbedderConfig, validateEmbedderConfig, deepEqual,
	getAppliedConfig, setAppliedConfigs, clearAppliedConfig,
} = require('./embedder');
const {
	EMBEDDER_TASK_TIMEOUT_MS, EMBEDDER_TASK_BG_TIMEOUT_MS, EMBEDDER_TASK_BG_INTERVAL_MS,
	MEILI_HTTP_TIMEOUT_MS,
} = require('./constants');

async function ensureIndex(plugin, uid, primaryKey) {
	try {
		await plugin.client.getIndex(uid);
	} catch (e) {
		await plugin.client.createIndex(uid, { primaryKey });
	}
}

// Meilisearch settings/document mutations are enqueued tasks - the initial call resolving
// just means Meilisearch accepted the request, not that it succeeded. waitForTask() itself
// only ever throws on ITS OWN polling timeout (confirmed against the installed
// meilisearch-js's TaskClient#waitForTask, node_modules/meilisearch/dist/index.js): a task
// that fails on the server resolves normally with status "failed" and an `error` object, so
// callers that don't check `.status` (as every waitForTask call in this plugin used to)
// silently treat a failed settings/document change as if it had gone through.
async function waitForSucceededTask(plugin, taskUidOrTask, options) {
	const task = await plugin.client.tasks.waitForTask(taskUidOrTask, options);
	if (task.status !== 'succeeded') {
		throw new Error(task.error?.message || `Meilisearch task ${task.status}`);
	}
	return task;
}

// Module-level tracker for in-flight embedder background pollers. Keyed by taskUid so the
// same task can't be polled twice (e.g. if updateEmbedders is somehow re-entered during
// the 60s sync wait). Cleared on terminal status (success or failure) by the poller itself.
const embedderPollers = new Set();

// Background-verify an embedder task that exceeded the sync wait window. Per the Meilisearch
// SDK (node_modules/meilisearch/dist/index.js:152), a poll timeout throws
// MeilisearchTaskTimeOutError — task is still running server-side. Re-embedding every
// document legitimately exceeds 60s on a forum of any size; the task may yet succeed.
// Strategy: poll at a longer interval with a longer timeout; alert only on delayed failure.
// On delayed success: fingerprint was already optimistically persisted at enqueue time, so
// nothing to do. On delayed failure: revert the optimistic fingerprint via clearAppliedConfig
// so the next save pushes fresh config, alert admins, flip plugin.healthy=false to defer
// subsequent writes to the pending queue.
function startEmbedderPoller(plugin, plan, taskUid) {
	if (embedderPollers.has(taskUid)) return;
	embedderPollers.add(taskUid);
	const startTime = Date.now();
	const poll = async () => {
		try {
			const task = await plugin.client.tasks.waitForTask(taskUid, {
				timeout: EMBEDDER_TASK_BG_TIMEOUT_MS,
				interval: EMBEDDER_TASK_BG_INTERVAL_MS,
			});
			embedderPollers.delete(taskUid);
			if (task.status !== 'succeeded') {
				// Delayed failure: fingerprint was optimistically persisted at enqueue time
				// (see plugin.updateEmbedders catch-block). Clear it so the next save pushes
				// fresh config rather than silently skipping as "already applied".
				await clearAppliedConfig(plugin, plan.indexName);
				plugin.healthy = false;
				plugin.notifyAdmins(`embedder:${plan.indexName}`, {
					type: 'danger',
					titleKey: '[[meilisearch:admin.semanticEmbedderDelayedFailed]]',
					message: `${plan.indexName}: ${task.error?.message || `task ${task.status}`}`,
				});
			} else {
				winston.info(`[plugin/meilisearch] Embedder task for "${plan.indexName}" succeeded after ${Date.now() - startTime}ms (background-poll)`);
			}
		} catch (err) {
			embedderPollers.delete(taskUid);
			await clearAppliedConfig(plugin, plan.indexName);
			plugin.healthy = false;
			plugin.notifyAdmins(`embedder:${plan.indexName}`, {
				type: 'danger',
				titleKey: '[[meilisearch:admin.semanticEmbedderDelayedFailed]]',
				message: `${plan.indexName}: ${err.message}`,
			});
		}
	};
	poll().catch(err => winston.error(`[plugin/meilisearch] embedder poller crashed: ${err.message}`));
}

// Recover in-flight embedder pollers after process restart. embedderPollers (line 42) is
// module-level in-memory; a NodeBB restart/crash/container-redeploy during the 30-min
// background-poller window (EMBEDDER_TASK_BG_TIMEOUT_MS) wipes it. Left unmitigated, the
// optimistic fingerprint persisted by updateEmbedders stays applied silently — even if the
// task failed server-side — so the next ACP save with unchanged semantic keys would skip
// the push as "already applied". This sweep runs once on prepareSearch to reattach pollers
// for any in-flight settings-update tasks on the plugin's 3 indexes that have a persisted
// appliedEmbedders fingerprint (the signature of an optimistic-pending state).
async function reattachPollers(plugin) {
	if (!plugin.client) return;
	try {
		const tasksResp = await plugin.client.tasks.getTasks({
			statuses: ['enqueued', 'processing'],
			types: ['settingsUpdate'],
			limit: 50,
		});
		const ourIndexes = ['post', 'topic', 'chat_message'];
		const candidates = (tasksResp.results || []).filter(t =>
			ourIndexes.includes(t.indexUid) && !embedderPollers.has(t.uid),
		);
		if (!candidates.length) return;
		winston.info(`[plugin/meilisearch] Reattaching ${candidates.length} embedder poller(s) after restart`);
		for (const task of candidates) {
			// Only reattach if there's a persisted applied fingerprint for this index —
			// otherwise this isn't the optimistic-pending case and reattaching would create
			// alerts for tasks that never had a fingerprint to clear.
			const applied = await getAppliedConfig(plugin, task.indexUid);
			if (applied === undefined) continue;
			// Reconstruct a minimal plan: indexName for clearAppliedConfig/notifyAdmins,
			// config for logging context (not used by the poller's control flow).
			const config = await buildEmbedderConfig(plugin, null, task.indexUid);
			startEmbedderPoller(plugin, { indexName: task.indexUid, config }, task.uid);
		}
	} catch (err) {
		// Non-fatal: if Meilisearch is unreachable or the tasks API errors, we just skip
		// the sweep. The worst case is the pre-fix silent-skip behavior, which self-heals
		// on the next legitimate semantic-key change.
		winston.warn(`[plugin/meilisearch] reattachPollers sweep failed (non-fatal): ${err.message}`);
	}
}

// MeiliSearch connection lifecycle: connecting, health checks, and index/settings sync.
module.exports = function attachClient(plugin) {
	plugin.patchTopicsSearch = function () {
		if (Topics.__meiliOriginalSearch) { return; }
		Topics.__meiliOriginalSearch = Topics.search;
		Topics.search = async function (tid, term) {
			if (!tid || !term || !String(term).trim()) { return []; }
			return Topics.__meiliOriginalSearch.call(Topics, tid, term);
		};
	};

	plugin.checkHealth = async function () {
		try {
			plugin.healthy = await plugin.client.isHealthy();
		if (!plugin.healthy) {
			winston.warn('[plugin/meilisearch] MeiliSearch host is unhealthy');
		} else {
			drainPending(plugin).catch(err => winston.error(`[plugin/meilisearch] drain failed: ${err.message}`));
			// G1: Recover in-flight embedder pollers after process restart.
			reattachPollers(plugin).catch(err => winston.warn(`[plugin/meilisearch] reattachPollers failed: ${err.message}`));
		}
			return plugin.healthy;
		} catch (err) {
			plugin.healthy = false;
			winston.warn(`[plugin/meilisearch] ${err.message}`);
			return false;
		}
	};

	// Ensure the chat_message index + filterable attributes exist, independent of reindex state.
	// prepareSearch() returns early when `indexed=true`, so updateIndexSettings (which configures
	// chat_message) is skipped on normal restarts — but indexMessage auto-creates the index via
	// document add, leaving filterableAttributes empty. Without roomId/uid filterable, searchMessages
	// would error on its filter clause. Idempotent + cheap (2 calls, settings deduped by Meilisearch).
	plugin.ensureChatMessageIndex = async function () {
		if (!plugin.client) return;
		try {
			await ensureIndex(plugin, 'chat_message', 'mid');
			await plugin.client.index('chat_message').updateFilterableAttributes(['roomId', 'uid', 'timestamp']);
		} catch (err) {
			winston.error(`[plugin/meilisearch] ensureChatMessageIndex failed: ${err.message}`);
			plugin.healthy = false;
		}
	};

	plugin.prepareSearch = async function (data, connectionChanged = false) {
		winston.debug(`[plugin/meilisearch] Connecting to MeiliSearch host: ${data?.host || await settings.getOne(plugin.id, 'host')}`);
		const { Meilisearch } = await import('meilisearch');
		plugin.client = new Meilisearch({
			host: data?.host || await settings.getOne(plugin.id, 'host'),
			apiKey: data?.apiKey || await settings.getOne(plugin.id, 'apiKey') || undefined,
			timeout: MEILI_HTTP_TIMEOUT_MS,
		});
		if (plugin.healthCheckTask) clearInterval(plugin.healthCheckTask);
		const intervalRaw = parseInt(data?.healthCheckInterval || await settings.getOne(plugin.id, 'healthCheckInterval'), 10);
		const intervalSeconds = Number.isFinite(intervalRaw) && intervalRaw > 0
			? Math.min(Math.max(intervalRaw, 10), 3600)
			: 60;
		plugin.healthCheckTask = setInterval(plugin.checkHealth, intervalSeconds * 1000);
		// #15: Always sync plugin.healthy on (re)connect — otherwise it stays false from
		// initialization until the first setInterval tick, causing stale "MeiliSearch
		// Unreachable" warnings on save during the startup window (MS online, flag stale).
		await plugin.checkHealth();
		// #14: Don't auto-reindex after a failed reindex unless connection settings changed.
		const indexed = await settings.getOne(plugin.id, 'indexed');
		if (indexed) return;
		if (!plugin.healthy || plugin.initializingOnAnotherInstance) return;
		const lastResult = await settings.getOne(plugin.id, 'lastReindexResult') || {};
		const allowAutoReindex = connectionChanged || lastResult.success !== false;
		if (!allowAutoReindex) {
			winston.warn('[plugin/meilisearch] Skipping auto-reindex: last reindex failed. Trigger manually from ACP.');
			return;
		}
		await plugin.updateIndexSettings();
		await plugin.updateEmbedders();
		await plugin.reindex(false);
	};

	// Keyword-search index settings only (ranking rules, stop words, typo tolerance,
	// synonyms, pagination). Deliberately does NOT touch embedders - see updateEmbedders
	// below for why that has to stay a separate, independently-gated call.
	plugin.updateIndexSettings = async (data) => {
		await ensureIndex(plugin, 'post', 'pid');
		await ensureIndex(plugin, 'topic', 'tid');
		await ensureIndex(plugin, 'chat_message', 'mid');
		data = {
			maxDocuments: parseInt(data?.maxDocuments || await settings.getOne(plugin.id, 'maxDocuments') || 500, 10),
			rankingRules: (data?.rankingRules || await settings.getOne(plugin.id, 'rankingRules'))?.map(value => value.rule),
			stopWords: (data?.stopWords || await settings.getOne(plugin.id, 'stopWords'))?.map(value => value.word),
			typoTolerance: ['on', true].includes(
				data?.typoTolerance || await settings.getOne(plugin.id, 'typoTolerance') || undefined,
			),
			typoToleranceMinWordSizeOneTypo: parseInt(
				data?.typoToleranceMinWordSizeOneTypo ||
					await settings.getOne(plugin.id, 'typoToleranceMinWordSizeOneTypo') || 5,
				10,
			),
			typoToleranceMinWordSizeTwoTypos: parseInt(
				data?.typoToleranceMinWordSizeTwoTypos ||
					await settings.getOne(plugin.id, 'typoToleranceMinWordSizeTwoTypos') || 9,
				10,
			),
			typoToleranceDisableOnWords:
				(data?.typoToleranceDisableOnWords || await settings.getOne(plugin.id, 'typoToleranceDisableOnWords') ||
					undefined)?.map(value => value.word),
			synonyms: Object.fromEntries(
				(data?.synonyms || await settings.getOne(plugin.id, 'synonyms') || [])?.map((
					{ word, synonyms },
				) => [word, synonyms?.split(',').map(synonym => synonym.trim())]),
			),
		};
		const postTask = await plugin.client.index('post').updateSettings({
			filterableAttributes: ['tid', 'cid', 'uid', 'timestamp'],
			sortableAttributes: ['timestamp', 'cid'],
			searchableAttributes: ['content'],
			pagination: {
				maxTotalHits: data.maxDocuments,
			},
			rankingRules: data.rankingRules,
			stopWords: data.stopWords,
			typoTolerance: {
				enabled: data.typoTolerance,
				minWordSizeForTypos: {
					oneTypo: data.typoToleranceMinWordSizeOneTypo,
					twoTypos: data.typoToleranceMinWordSizeTwoTypos,
				},
				disableOnWords: data.typoToleranceDisableOnWords,
			},
			synonyms: data.synonyms,
		});
		const topicTask = await plugin.client.index('topic').updateSettings({
			filterableAttributes: ['cid', 'uid', 'timestamp'],
			sortableAttributes: ['cid', 'title', 'timestamp'],
			searchableAttributes: ['title'],
			pagination: {
				maxTotalHits: data.maxDocuments,
			},
			rankingRules: data.rankingRules,
			stopWords: data.stopWords,
			typoTolerance: {
				enabled: data.typoTolerance,
				minWordSizeForTypos: {
					oneTypo: data.typoToleranceMinWordSizeOneTypo,
					twoTypos: data.typoToleranceMinWordSizeTwoTypos,
				},
				disableOnWords: data.typoToleranceDisableOnWords,
			},
			synonyms: data.synonyms,
		});
		const messageTask = await plugin.client.index('chat_message').updateSettings({
			filterableAttributes: ['roomId', 'uid', 'timestamp'],
			sortableAttributes: ['timestamp'],
			searchableAttributes: ['content'],
			pagination: {
				maxTotalHits: data.maxDocuments,
			},
			rankingRules: data.rankingRules,
			stopWords: data.stopWords,
			typoTolerance: {
				enabled: data.typoTolerance,
				minWordSizeForTypos: {
					oneTypo: data.typoToleranceMinWordSizeOneTypo,
					twoTypos: data.typoToleranceMinWordSizeTwoTypos,
				},
				disableOnWords: data.typoToleranceDisableOnWords,
			},
			synonyms: data.synonyms,
		});
		return [postTask, topicTask, messageTask];
	};

	// Pushes (or removes) the "default" embedder on all three indexes based on the
	// semantic search ACP settings. Meilisearch re-embeds every existing document
	// through the (often paid) embedding API whenever an index's embedder config
	// actually changes, so this is intentionally its own call rather than something
	// updateIndexSettings does automatically - it must only run when semantic settings
	// were deliberately changed (see lib/settings.js), or as part of an explicit
	// reindex/connect flow, never as a side effect of unrelated settings (ranking rules,
	// stop words, ...) being saved. Skips the network call entirely when the resolved
	// config is identical to what was last pushed, as a second line of defense against
	// needless re-embedding regardless of caller. Older/self-hosted Meilisearch instances
	// without embedder support will reject the call; that's caught here so it never
	// blocks the rest of the settings-save flow.
	plugin.updateEmbedders = async (rawData) => {
		// Pre-flight validation: fail fast with an admin-facing alert instead of letting
		// Meilisearch reject the task server-side (which can take up to 60s on slow/unreachable
		// embedding endpoints, leaving the ACP Save button stuck). Client-side validation in
		// static/lib/admin.js is the primary UX path; this is defense-in-depth for reindex,
		// prepareSearch-on-host-change, and any programmatic caller.
		const validationError = await validateEmbedderConfig(plugin, rawData);
		if (validationError) {
			winston.warn(`[plugin/meilisearch] Embedder config validation failed: ${validationError}`);
			plugin.notifyAdmins('embedder:validation', {
				type: 'danger',
				titleKey: '[[meilisearch:admin.semanticConfigInvalid]]',
				message: validationError,
			});
			return;  // no fingerprint persisted, no task enqueued, save proceeds immediately
		}
		const indexNames = ['post', 'topic', 'chat_message'];
		// Resolve configs + diff-against-applied first (read-only), THEN push and persist -
		// keeps the three indexes from racing on the same shared "applied" bookkeeping object
		// (parallel read-modify-write on one settings key would let one index's write silently
		// clobber another's).
		const plans = await Promise.all(indexNames.map(async (indexName) => {
			const config = await buildEmbedderConfig(plugin, rawData, indexName);
			const applied = await getAppliedConfig(plugin, indexName);
			return { indexName, config, needsPush: applied === undefined || !deepEqual(config, applied) };
		}));
		const results = await Promise.all(plans.map(async (plan) => {
			if (!plan.needsPush) return null;
			try {
				// updateEmbedders() only enqueues a Meilisearch task - a bad URL/model/response
				// shape for "rest"/"openAi"-with-custom-url configs only surfaces once Meilisearch
				// actually runs the task (e.g. its probe call to auto-detect dimensions), so this
				// must wait for and check the task's real outcome, not just the enqueue response.
				const enqueued = await plugin.client.index(plan.indexName).updateEmbedders({ [EMBEDDER_NAME]: plan.config });
				try {
					await waitForSucceededTask(plugin, enqueued.taskUid, {
						timeout: EMBEDDER_TASK_TIMEOUT_MS,
						interval: 200,
					});
				} catch (err) {
					// Distinguish "task is still running" (poll timeout) from "task definitively failed"
					// (Meili rejected the call, 4xx, malformed config, network error). Meilisearch SDK
					// throws MeilisearchTaskTimeOutError (name property, see node_modules/meilisearch/dist/index.js:152)
					// on poll timeout; this means the task is still running server-side and may yet succeed.
					if (err.name === 'MeilisearchTaskTimeOutError') {
						// Re-embedding every document legitimately exceeds 60s on a forum of any size.
						// Optimistically persist the fingerprint so the next ACP save doesn't re-push
						// identical config (which would double paid-API cost — the exact thing we're
						// preventing). startEmbedderPoller verifies the real outcome.
						winston.warn(`[plugin/meilisearch] Embedder task for "${plan.indexName}" still running after ${EMBEDDER_TASK_TIMEOUT_MS}ms; background-polling.`);
						startEmbedderPoller(plugin, plan, enqueued.taskUid);
						return plan;  // optimistic: treat as succeeded for fingerprint persistence
					}
					throw err;  // real failure — outer catch handles alerting + return null
				}
				return plan;
			} catch (err) {
				const reason = err.message;
				winston.error(`[plugin/meilisearch] Failed to apply "${plan.indexName}" embedder (semantic search): ${reason}`);
				plugin.notifyAdmins(`embedder:${plan.indexName}`, {
					type: 'danger',
					titleKey: '[[meilisearch:admin.semanticEmbedderFailed]]',
					message: `${plan.indexName}: ${reason}`,
				});
				return null;
			}
		}));
		const succeeded = results.filter(Boolean);
		if (succeeded.length) {
			await setAppliedConfigs(plugin, succeeded);
		}
	};
};

// Exposed so other call sites that enqueue Meilisearch tasks (lib/reindex.js) get the same
// "actually check the task succeeded" behavior instead of trusting waitForTask() to throw.
module.exports.waitForSucceededTask = waitForSucceededTask;
