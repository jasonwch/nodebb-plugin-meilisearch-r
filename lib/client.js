'use strict';

const winston = nodebb.require('winston');
const settings = nodebb.require('./src/meta/settings');
const nconf = nodebb.require('nconf');
const db = nodebb.require('./src/database');
const pubsub = nodebb.require('./src/pubsub');
const cache = nodebb.require('./src/cache');
const Topics = nodebb.require('./src/topics');
const { drainPending } = require('./pending-queue');
const {
	EMBEDDER_NAME, buildEmbedderConfig, validateEmbedderConfig, deepEqual,
	getAppliedConfig, getAppliedTaskUid, setAppliedConfigs, clearAppliedConfig,
} = require('./embedder');
const {
	EMBEDDER_TASK_TIMEOUT_MS, EMBEDDER_TASK_BG_TIMEOUT_MS, EMBEDDER_TASK_BG_INTERVAL_MS,
	MEILI_HTTP_TIMEOUT_MS, REINDEX_LOCK_KEY, REINDEX_LOCK_TTL, LOCK_REFRESH_INTERVAL, INDEXING_STALE_MS,
} = require('./constants');
const { isReindexActive } = require('./pending-queue');
const { redactSecrets } = require('./redact');
const { isSettingTrue } = require('./settings-helpers');
const { getJsonSetting } = require('./settings-json');
const { GLOBAL_CHAT_SEARCH_FETCH } = require('./constants');

function hasOwn(obj, key) {
	return !!obj && Object.prototype.hasOwnProperty.call(obj, key);
}

// F10: resolve a setting from an in-progress save payload WITHOUT a `||` chain.
// filter:settings.set fires BEFORE core persists the save, so settings.getOne()
// still returns the PRE-save value during the hook — `data?.apiKey || getOne()`
// therefore resurrected the old stored value whenever the admin CLEARED a field
// (empty string is falsy), keeping a rotated-out credential live until restart.
async function resolveSetting(plugin, data, key) {
	return hasOwn(data, key) ? data[key] : await settings.getOne(plugin.id, key);
}

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
// On delayed success: update the persisted taskUid to null (sync-verified) so reattachPollers
//   skips it on future restarts instead of hitting a 404 when Meilisearch purges task history.
// On delayed failure: revert the optimistic fingerprint via clearAppliedConfig so the next
//   save pushes fresh config, alert admins, flip plugin.healthy=false to defer subsequent
//   writes to the pending queue.
// On poll timeout (MeilisearchTaskTimeOutError): task is STILL RUNNING — do NOT clear the
//   fingerprint (would re-open the double-paid-API-cost loop). Log and let the task continue
//   server-side; if it ultimately fails, the next restart's reattachPollers will catch it.
// On network error: task status is genuinely unknown — log warning but do NOT clear, since
//   clearing on a transient blip would cause an unnecessary re-push. reattachPollers catches
//   actual failures on the next restart.
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
				// C1c fix: redact any apiKey that Meilisearch may echo in task.error.message before
				// sending to admin toasts. Issue 2 admin-room filter already limits toast recipients,
				// but defense-in-depth handles the case where Meilisearch wraps the apiKey in the error.
				// A3-style inner try/catch: if redaction itself fails, fall back to raw reason rather
				// than crashing the poller's failure-handling path.
				const rawReason = task.error?.message || `task ${task.status}`;
				let safeReason;
				try {
					safeReason = await redactSecrets(rawReason, plugin);
				} catch {
					safeReason = rawReason;
				}
				await clearAppliedConfig(plugin, plan.indexName);
				plugin.healthy = false;
				plugin.notifyAdmins(`embedder:${plan.indexName}`, {
					type: 'danger',
					titleKey: '[[meilisearch:admin.semanticEmbedderDelayedFailed]]',
					message: `${plan.indexName}: ${safeReason}`,
				});
			} else {
				// Delayed success: mark taskUid as null (sync-verified) so reattachPollers
				// skips this index on future restarts instead of hitting a 404 when Meilisearch
				// purges task history (~24h). Fingerprint config is already correct — no change.
				winston.info(`[plugin/meilisearch] Embedder task for "${plan.indexName}" succeeded after ${Date.now() - startTime}ms (background-poll)`);
				await setAppliedConfigs(plugin, [{ indexName: plan.indexName, config: plan.config, taskUid: null }]);
			}
		} catch (err) {
			embedderPollers.delete(taskUid);
			if (err.name === 'MeilisearchTaskTimeOutError') {
				// Poller's own 30-min timeout fired — task is STILL RUNNING server-side.
				// Do NOT clear the fingerprint: that would make the next save re-push identical
				// config, re-opening the double-paid-API-cost loop this whole mechanism prevents.
				// The task will eventually succeed or fail; if NodeBB restarts before that,
				// reattachPollers will pick it up via the persisted taskUid.
				winston.warn(`[plugin/meilisearch] Embedder task ${taskUid} for "${plan.indexName}" still running after ${EMBEDDER_TASK_BG_TIMEOUT_MS}ms; giving up poll. Task continues server-side — reattachPollers will reconcile on next restart.`);
				return;
			}
			// Network error or other non-timeout failure — task status is genuinely unknown.
			// C1 fix: redact any apiKey echoed in err.message before logging.
			// A3-style inner try/catch: fall back to raw err.message if redaction itself fails.
			const rawPollerErr = err.message;
			let safePollerErr;
			try {
				safePollerErr = await redactSecrets(rawPollerErr, plugin);
			} catch {
				safePollerErr = rawPollerErr;
			}
			winston.warn(`[plugin/meilisearch] Embedder poller for "${plan.indexName}" (task ${taskUid}) hit an error (non-fatal, fingerprint preserved): ${safePollerErr}`);
		}
	};
	poll().catch(async err => {
		// A3: inner try/catch makes the redaction layer airtight against future regressions
		// where redactSecrets itself might throw (currently fully guarded internally, but
		// defense-in-depth ensures the log path still runs).
		let safeCrashErr;
		try {
			safeCrashErr = await redactSecrets(err.message, plugin);
		} catch {
			safeCrashErr = '<redaction failed>';
		}
		winston.error(`[plugin/meilisearch] embedder poller crashed: ${safeCrashErr}`);
	});
}

// Recover embedder pollers after process restart. embedderPollers (line 42) is module-level
// in-memory; a NodeBB restart/crash/container-redeploy during the 30-min background-poller
// window (EMBEDDER_TASK_BG_TIMEOUT_MS) wipes it. This sweep reads the persisted taskUid
// (stored alongside each fingerprint in appliedEmbedders via setAppliedConfigs) and
// reconciles directly via plugin.client.tasks.getTask(uid):
//   succeeded → no-op (fingerprint correct, taskUid was null from sync-success path)
//   enqueued/processing → reattach startEmbedderPoller (task still running server-side)
//   failed → clearAppliedConfig + alert (Scenario A: task failed during downtime)
//   404/network error → assume success, log warning (self-heals on next config change)
// For old-format fingerprints (no taskUid), falls through to query-based sweep as fallback.
async function reattachPollers(plugin) {
	if (!plugin.client) return;
	const ourIndexes = ['post', 'topic', 'chat_message'];
	// Phase 1: Direct reconciliation via persisted taskUid (new-format fingerprints)
	for (const indexName of ourIndexes) {
		try {
			const taskUid = await getAppliedTaskUid(plugin, indexName);
			if (taskUid === undefined || taskUid === null) continue;
			const task = await plugin.client.tasks.getTask(taskUid);
			if (task.status === 'succeeded') continue;
			if (task.status === 'enqueued' || task.status === 'processing') {
				if (embedderPollers.has(taskUid)) continue;
				const config = await buildEmbedderConfig(plugin, null, indexName);
				winston.info(`[plugin/meilisearch] Reattaching embedder poller for "${indexName}" (task ${taskUid} still ${task.status})`);
				startEmbedderPoller(plugin, { indexName, config }, taskUid);
				continue;
			}
			// Task failed during downtime — clear stale fingerprint + alert
			// C1b fix: redact any apiKey from task.error.message before logging + alerting.
			// A3-style inner try/catch: fall back to raw reason if redaction itself fails.
			const rawFailReason = task.error?.message || `task ${task.status}`;
			let safeFailReason;
			try {
				safeFailReason = await redactSecrets(rawFailReason, plugin);
			} catch {
				safeFailReason = rawFailReason;
			}
			winston.warn(`[plugin/meilisearch] Embedder task ${taskUid} for "${indexName}" failed during downtime: ${safeFailReason}`);
			await clearAppliedConfig(plugin, indexName);
			plugin.healthy = false;
			plugin.notifyAdmins(`embedder:${indexName}`, {
				type: 'danger',
				titleKey: '[[meilisearch:admin.semanticEmbedderDelayedFailed]]',
				message: `${indexName}: ${safeFailReason}`,
			});
		} catch (err) {
			// Non-fatal: taskUid may be 404 (purged from Meilisearch's ~24h history), or
			// Meilisearch may be momentarily unreachable. Assume success — self-heals on next
			// legitimate semantic-key change.
			winston.warn(`[plugin/meilisearch] reattachPollers: could not reconcile taskUid for "${indexName}" (non-fatal): ${err.message}`);
		}
	}
	// Phase 2: Query-based fallback for old-format fingerprints (no taskUid persisted).
	// Catches in-flight embedder tasks that have a persisted fingerprint but no taskUid
	// (pre-Option-1 format). Will be a no-op once all fingerprints are migrated to new format.
	try {
		const tasksResp = await plugin.client.tasks.getTasks({
			statuses: ['enqueued', 'processing'],
			types: ['settingsUpdate'],
			limit: 50,
		});
		const candidates = [];
		for (const t of (tasksResp.results || [])) {
			if (!ourIndexes.includes(t.indexUid)) continue;
			if (embedderPollers.has(t.uid)) continue;
			if (!(t.details && t.details.embedders)) continue;
			const tu = await getAppliedTaskUid(plugin, t.indexUid);
			if (tu !== undefined) continue;  // new-format fingerprint — already reconciled in Phase 1
			candidates.push(t);
		}
		if (!candidates.length) return;
		winston.info(`[plugin/meilisearch] Query fallback: reattaching ${candidates.length} embedder poller(s) for old-format fingerprints`);
		for (const task of candidates) {
			const applied = await getAppliedConfig(plugin, task.indexUid);
			if (applied === undefined) continue;
			const config = await buildEmbedderConfig(plugin, null, task.indexUid);
			startEmbedderPoller(plugin, { indexName: task.indexUid, config }, task.uid);
		}
	} catch (err) {
		winston.warn(`[plugin/meilisearch] reattachPollers query fallback failed (non-fatal): ${err.message}`);
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
				// B2b: SDK write/health error path — drainPending replay errors may contain
				// document content snippets from already-indexed posts (rare; only if
				// Meilisearch rejects a batch quoting the bad doc). Accepted risk for
				// low-frequency replay failures; document content is already-in-DB data.
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
			// B2b: SDK settings error path — updateFilterableAttributes errors don't typically
			// echo credentials or document content; accepted risk for low-frequency settings failures.
			winston.error(`[plugin/meilisearch] ensureChatMessageIndex failed: ${err.message}`);
			plugin.healthy = false;
		}
	};

	plugin.prepareSearch = async function (data, connectionChanged = false) {
		// F10: hasOwnProperty-based resolution — a field the admin CLEARED ('') must
		// reach the client as its cleared value, not silently fall back to the
		// pre-save stored value.
		const host = await resolveSetting(plugin, data, 'host');
		const apiKey = await resolveSetting(plugin, data, 'apiKey');
		const intervalSetting = await resolveSetting(plugin, data, 'healthCheckInterval');
		winston.debug(`[plugin/meilisearch] Connecting to MeiliSearch host: ${host}`);
		if (!host) {
			// Nothing sensible to connect to (cleared or never configured). Degrade
			// loudly instead of constructing a client against an empty host.
			winston.error('[plugin/meilisearch] No MeiliSearch host configured — search/indexing disabled until one is set');
			plugin.healthy = false;
			return;
		}
		const { Meilisearch } = await import('meilisearch');
		plugin.client = new Meilisearch({
			host,
			apiKey: apiKey || undefined,
			timeout: MEILI_HTTP_TIMEOUT_MS,
		});
		if (plugin.healthCheckTask) clearInterval(plugin.healthCheckTask);
		const intervalRaw = parseInt(intervalSetting, 10);
		const intervalSeconds = Number.isFinite(intervalRaw) && intervalRaw > 0
			? Math.min(Math.max(intervalRaw, 10), 3600)
			: 60;
		plugin.healthCheckTask = setInterval(plugin.checkHealth, intervalSeconds * 1000);
		// #15: Always sync plugin.healthy on (re)connect — otherwise it stays false from
		// initialization until the first setInterval tick, causing stale "MeiliSearch
		// Unreachable" warnings on save during the startup window (MS online, flag stale).
		await plugin.checkHealth();
		// G1: Recover in-flight embedder pollers after process restart (runs once on startup,
		// not on every health-check tick — the sweep is unnecessary after the first run since
		// embedderPollers Set is populated and deduplicates subsequent calls).
		reattachPollers(plugin).catch(err => winston.warn(`[plugin/meilisearch] reattachPollers failed: ${err.message}`));
		// Seed the indexing state from the reindex lock: a worker booting mid-run has
		// no pubsub history and would otherwise write directly against the running
		// reindex for up to a keepalive interval (batch emits are ~500ms during Phase 1,
		// but the setup phase is only covered by the 30s keepalive). FLAW 5 FIX: seed
		// only from a provably FRESH lock. A live holder refreshes the TTL every
		// LOCK_REFRESH_INTERVAL, so its TTL stays within one or two refresh intervals
		// of the max; a dead holder's TTL decays monotonically. Seeding from a decayed
		// lock re-armed 15-minute write-deferral windows on every prepareSearch call
		// (settings saves, restarts, connection changes) for up to the 60-min TTL —
		// recurring search-staleness with no run behind it. The floor allows two
		// missed refreshes (a struggling-but-alive holder must never fail to seed:
		// an unseeded worker writes directly during a live run — the stale-overwrite
		// race this deferral exists to prevent); anything below it is either dead or
		// dead-enough that the staleness self-heal (15 min) has already resumed
		// direct writes. The boot auto-reindex below is skipped ENTIRELY for this
		// prepareSearch invocation when we do seed — the seeded guard refuses it and
		// nothing re-runs prepareSearch after the heal — which is why the one-shot
		// retry below exists.
		if (!plugin.indexing.running) {
			try {
				const lockTtl = await db.ttl(REINDEX_LOCK_KEY);
				const seedFloorTtl = REINDEX_LOCK_TTL - Math.ceil((2 * LOCK_REFRESH_INTERVAL) / 1000);
				if (lockTtl >= seedFloorTtl) {
					plugin.indexing.running = true;
					plugin.indexingUpdatedAt = Date.now();
					winston.info('[plugin/meilisearch] Reindex lock held — seeding indexing state (mid-run boot or stale lock)');
					// One-shot auto-reindex retry: after INDEXING_STALE_MS the seeded
					// state has gone stale (or a live run has finished and set indexed),
					// so this either reindexes (lock expired) or surfaces the held lock
					// via the lock-refusal admin alert. One retry per process — repeated
					// prepareSearch calls (settings saves) don't stack timers.
					// Primary-gated like the normal boot auto-reindex: without this, a
					// clustered boot against a lock that expires mid-window synchronizes
					// N retry timers into the lock's check-then-set window — the exact
					// boot storm the isPrimary guard exists to prevent. The SEED above
					// stays ungated: every worker needs write deferral.
					if (nconf.get('isPrimary') && !plugin.autoReindexRetryScheduled) {
						plugin.autoReindexRetryScheduled = true;
						const retry = setTimeout(() => {
							(async () => {
								try {
									// F7: Redis stores this flag as a STRING — a bare
									// truthiness check treated a stored 'false' as
									// "already indexed" and consumed the retry for nothing.
									if (isSettingTrue(await settings.getOne(plugin.id, 'indexed'))) {
										return; // a real run completed in the meantime
									}
									// Health double-check (maybeDefer pattern): a stale
									// healthy=false re-verifies with one call before
									// consuming the one-shot; a genuinely unreachable
									// Meilisearch skips here instead of hanging
									// updateIndexSettings on 30s SDK timeouts — and never
									// runs a reindex that would fail and persist
									// success:false (tripping the #14 boot guard until
									// manual action).
									if (!plugin.healthy && !(await plugin.checkHealth())) {
										winston.warn('[plugin/meilisearch] Skipping the auto-reindex retry — Meilisearch is unreachable');
										return;
									}
									if (plugin.indexing.running && isReindexActive(plugin)) {
										return; // a live run took over via pubsub
									}
									winston.info('[plugin/meilisearch] Retrying the auto-reindex after the seeding window');
									// F1: fire-and-forget — runReindex performs its own
									// updateIndexSettings/updateEmbedders setup; awaiting it
									// here would stall this timer's (and the boot's) callers.
									plugin.reindex(false).catch(err =>
										winston.error(`[plugin/meilisearch] Auto-reindex retry failed: ${err.message}`));
								} catch (err) {
									winston.error(`[plugin/meilisearch] Auto-reindex retry failed: ${err.message}`);
								}
							})();
						}, INDEXING_STALE_MS);
						if (retry.unref) {
							retry.unref();
						}
					}
				} else if (lockTtl > 0) {
					// Decayed lock: a live holder's TTL never drops this low (it refreshes
					// every LOCK_REFRESH_INTERVAL). Treat as dead — the staleness
					// self-heal already resumed direct writes, and re-seeding here is
					// what re-armed the deferral loop.
					winston.info(`[plugin/meilisearch] Reindex lock present but decayed (TTL ${lockTtl}s < ${seedFloorTtl}s) — not seeding indexing state`);
				}
			} catch (err) {
				winston.warn(`[plugin/meilisearch] Could not check the reindex lock for state seeding: ${err.message}`);
			}
		}
		// #14: Don't auto-reindex after a failed reindex unless connection settings changed.
		// F7: isSettingTrue — Redis stringifies the flag, so a bare truthiness check read
		// a stored 'false' as "already indexed" and silently skipped the auto-reindex.
		const indexed = await settings.getOne(plugin.id, 'indexed');
		if (isSettingTrue(indexed)) return;
		if (!plugin.healthy || plugin.initializingOnAnotherInstance) return;
		// F8: JSON-tolerant read — on Redis this was the string '[object Object]', whose
		// `.success` is undefined (so the guard below passed by accident).
		const lastResult = await getJsonSetting(plugin, 'lastReindexResult', {});
		const allowAutoReindex = connectionChanged || lastResult.success !== false;
		if (!allowAutoReindex) {
			winston.warn('[plugin/meilisearch] Skipping auto-reindex: last reindex failed. Trigger manually from ACP.');
			return;
		}
		// Boot-storm guard: on a clustered NodeBB, every worker's init lands here
		// within milliseconds when indexed=false, and simultaneous acquireReindexLock
		// attempts across workers can hit the lock's check-then-set window (double
		// run). Only the primary worker auto-reindexes on boot; an explicit
		// connection change (admin save, handled by one worker) always proceeds.
		if (!nconf.get('isPrimary') && !connectionChanged) {
			winston.info('[plugin/meilisearch] Skipping boot auto-reindex on non-primary worker (the primary worker will reindex)');
			return;
		}
		// F1: fire the auto-reindex in the BACKGROUND. Awaiting it here stalled
		// static:app.load (NodeBB binds its HTTP listener only after that hook
		// resolves) and the ACP host-change Save for the entire run + pending-queue
		// drain — a full outage window on single-process deployments. runReindex
		// performs its own updateIndexSettings/updateEmbedders setup (the calls here
		// were a redundant duplicate of it), and progress is already published over
		// pubsub for the ACP.
		// Claim-2 fix: when this run was triggered by an ACP save (host change),
		// thread the in-flight save payload into it. Core persists the form only
		// AFTER the filter:settings.set hook returns, so the reindex's own
		// updateIndexSettings/updateEmbedders reads would otherwise see the OLD
		// (pre-save) values and push a stale embedder config that — landing after
		// the save path's own push in meilisearch's FIFO — silently overwrote the
		// admin's new one.
		const reindexOptions = (data && connectionChanged) ? { settingsData: data } : undefined;
		plugin.reindex(false, reindexOptions).catch(err =>
			winston.error(`[plugin/meilisearch] Auto-reindex failed: ${err.message}`));
	};

	// Lightweight client rebuild for settings propagation — deliberately NOT the
	// full prepareSearch tail (no seeding, no auto-reindex: the saving instance
	// already handled those; a peer only needs to point at the new host).
	plugin.reconnectClient = async function (data = {}) {
		// F10: hasOwnProperty-based resolution (see resolveSetting) — a cleared field
		// must not resurrect the previously stored value on peer instances either.
		const host = await resolveSetting(plugin, data, 'host');
		const apiKey = await resolveSetting(plugin, data, 'apiKey');
		const intervalSetting = await resolveSetting(plugin, data, 'healthCheckInterval');
		if (!host) {
			winston.error('[plugin/meilisearch] No MeiliSearch host configured — peer client not rebuilt');
			return;
		}
		const { Meilisearch } = await import('meilisearch');
		plugin.client = new Meilisearch({
			host,
			apiKey: apiKey || undefined,
			timeout: MEILI_HTTP_TIMEOUT_MS,
		});
		if (plugin.healthCheckTask) clearInterval(plugin.healthCheckTask);
		const intervalRaw = parseInt(intervalSetting, 10);
		const intervalSeconds = Number.isFinite(intervalRaw) && intervalRaw > 0
			? Math.min(Math.max(intervalRaw, 10), 3600)
			: 60;
		plugin.healthCheckTask = setInterval(plugin.checkHealth, intervalSeconds * 1000);
		await plugin.checkHealth();
	};

	// CLUSTER SETTINGS PROPAGATION. NodeBB 4.16.3's meta/settings hash cache is
	// per-process (ttl: 0) with NO cross-instance invalidation — core publishes
	// action:settings.set.<hash> on pubsub (an event built for the legacy Settings
	// class, which nothing modern consumes) and the filter:settings.set hook —
	// this plugin's only other reaction path — fires on the SAVING worker alone.
	// Without this listener, peers keep pre-save settings and the OLD Meilisearch
	// client forever (until restart): stale search params, stale semantic toggles,
	// searches against a dead old host. Core's event also fires for this plugin's
	// own quiet bookkeeping sets (lastReindexResult etc.) with no quiet marker in
	// the payload, so: ALWAYS invalidate the local cache (peers then re-read fresh
	// values — including the reindex result card); rebuild the client only when
	// the payload's connection keys differ from the pre-invalidation cached values
	// — which also makes this a no-op on the saving instance (core invalidated its
	// cache before publishing, so it sees no diff).
	pubsub.on('action:settings.set.meilisearch', async (values) => {
		if (!plugin.initialized || !values || typeof values !== 'object') {
			return;
		}
		try {
			const hasConnectionKeys = values.host !== undefined || values.apiKey !== undefined;
			const connectionChanged = hasConnectionKeys && (
				String(await settings.getOne(plugin.id, 'host') || '') !== String(values.host ?? '') ||
				String(await settings.getOne(plugin.id, 'apiKey') || '') !== String(values.apiKey ?? '')
			);
			cache.del('settings:meilisearch');
			if (connectionChanged) {
				await plugin.reconnectClient(values);
				winston.info('[plugin/meilisearch] Meilisearch connection settings changed (settings broadcast) — client rebuilt on this instance');
			}
		} catch (err) {
			winston.warn(`[plugin/meilisearch] Could not apply the settings broadcast: ${err.message}`);
		}
	});

	// Keyword-search index settings only (ranking rules, stop words, typo tolerance,
	// synonyms, pagination). Deliberately does NOT touch embedders - see updateEmbedders
	// below for why that has to stay a separate, independently-gated call.
	plugin.updateIndexSettings = async (data) => {
		await ensureIndex(plugin, 'post', 'pid');
		await ensureIndex(plugin, 'topic', 'tid');
		await ensureIndex(plugin, 'chat_message', 'mid');
		// F9: an emptied ACP ranking-rules list arrives as [] — a TRUTHY empty array
		// that bypassed the `||` fallback and was sent to Meilisearch verbatim, storing
		// an empty rule set (silently degraded relevance). Treat an emptied list as
		// "omit the key" so the save becomes a no-op for ranking rules instead.
		const rawRankingRules = hasOwn(data, 'rankingRules') ? data.rankingRules : await settings.getOne(plugin.id, 'rankingRules');
		const rankingRules = (Array.isArray(rawRankingRules) && rawRankingRules.length)
			? rawRankingRules.map(value => value.rule)
			: undefined;
		data = {
			maxDocuments: parseInt(data?.maxDocuments || await settings.getOne(plugin.id, 'maxDocuments') || 500, 10),
			rankingRules,
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
				// F6: global chat search over-fetches GLOBAL_CHAT_SEARCH_FETCH hits to
				// compensate for join-timestamp/deleted-message filtering. Meilisearch
				// caps every read at maxTotalHits, so tying this index's cap to
				// maxDocuments silently truncated chat search whenever an admin set
				// "Max documents" below the over-fetch size.
				maxTotalHits: Math.max(GLOBAL_CHAT_SEARCH_FETCH, data.maxDocuments),
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
	// Claim-2 fix: serialize concurrent updateEmbedders invocations. A host-change
	// save fires a background reindex whose updateEmbedders raced the save path's
	// own push: both read the just-reset (empty) fingerprint and both pushed, the
	// reindex's stale read could even land AFTER the save's new one (meilisearch
	// applies tasks FIFO per index) and silently overwrite it, and the two
	// setAppliedConfigs writes raced last-writer-wins. Serializing lets the second
	// invocation re-read the fingerprint the first one persisted and skip as
	// "already applied" — one push per config change, whichever caller wins the race.
	plugin.updateEmbedders = async function (rawData) {
		while (plugin._embedderPushInFlight) {
			await plugin._embedderPushInFlight.catch(() => {});
		}
		const run = pushEmbedders(rawData);
		plugin._embedderPushInFlight = run;
		try {
			return await run;
		} finally {
			if (plugin._embedderPushInFlight === run) {
				plugin._embedderPushInFlight = null;
			}
		}
	};

	async function pushEmbedders(rawData) {
		// Pre-flight validation: fail fast with an admin-facing alert instead of letting
		// Meilisearch reject the task server-side (which can take up to 60s on slow/unreachable
		// embedding endpoints, leaving the ACP Save button stuck). Client-side validation in
		// static/lib/admin.js is the primary UX path; this is defense-in-depth for reindex,
		// prepareSearch-on-host-change, and any programmatic caller.
		const validationError = await validateEmbedderConfig(plugin, rawData);
		if (validationError) {
			winston.warn(`[plugin/meilisearch] Embedder config validation failed: ${validationError.text}`);
			plugin.notifyAdmins('embedder:validation', {
				type: 'danger',
				titleKey: '[[meilisearch:admin.semanticConfigInvalid]]',
				messageKey: validationError.key,
				messageArgs: validationError.args,
				message: validationError.text,
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
						return { ...plan, taskUid: enqueued.taskUid };  // optimistic — taskUid for later reconciliation
					}
					throw err;  // real failure — outer catch handles alerting + return null
				}
				return { ...plan, taskUid: null };  // sync-verified success — no reconciliation needed
			} catch (err) {
				// C1a fix: redact any apiKey from err.message before logging + alerting.
				// Meilisearch may echo the configured apiKey in 401/403 error responses.
				// A3-style inner try/catch: fall back to raw err.message if redaction itself fails,
				// preserving the catch block's responsibility (alert + return null).
				const rawReason = err.message;
				let reason;
				try {
					reason = await redactSecrets(rawReason, plugin);
				} catch {
					reason = rawReason;
				}
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
