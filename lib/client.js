'use strict';

const winston = nodebb.require('winston');
const settings = nodebb.require('./src/meta/settings');
const Topics = nodebb.require('./src/topics');
const { drainPending } = require('./pending-queue');
const {
	EMBEDDER_NAME, buildEmbedderConfig, deepEqual, getAppliedConfig, setAppliedConfigs,
} = require('./embedder');

async function ensureIndex(plugin, uid, primaryKey) {
	try {
		await plugin.client.getIndex(uid);
	} catch (e) {
		await plugin.client.createIndex(uid, { primaryKey });
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
				await plugin.client.index(plan.indexName).updateEmbedders({ [EMBEDDER_NAME]: plan.config });
				return plan;
			} catch (err) {
				winston.warn(`[plugin/meilisearch] Failed to update "${plan.indexName}" embedders (semantic search): ${err.message}`);
				return null;
			}
		}));
		const succeeded = results.filter(Boolean);
		if (succeeded.length) {
			await setAppliedConfigs(plugin, succeeded);
		}
	};
};
