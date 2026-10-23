'use strict';

const _ = nodebb.require('lodash');
const winston = nodebb.require('winston');
const pubsub = nodebb.require('./src/pubsub');
const settings = nodebb.require('./src/meta/settings');
const db = nodebb.require('./src/database');
const { PENDING_KEY, MEILI_DRAIN_POLL_MS, MEILI_DRAIN_SENTINEL_MS, MEILI_DRAIN_TIMEOUT_MS } = require('./constants');
const { getJsonSetting, setJsonSetting } = require('./settings-json');

const INDEX_UIDS = ['post', 'topic', 'chat_message'];

// After Phase 1 (enqueueing) finishes, Meilisearch still has to drain its task queue
// ("Phase 2"). This monitor publishes meili_progress (tasks processed / tasks enqueued)
// on the existing 'meilisearch:reindex' pubsub channel so the ACP renders a fourth
// progress bar, then — once the queue drains — verifies the run's outcome on the
// Meilisearch side (failed tasks + per-index document counts) and reports problems via
// the admin alert channel + lastReindexResult.verification. Read-only apart from those
// two reporting paths; fire-and-forget enqueueing is unchanged.
//
// Drain detection uses the run's LAST task status (Meilisearch processes tasks in uid
// order, so a terminal last task implies all of the run's tasks finished) — exact, and
// immune to foreign-task noise in the global backlog. When live writes were replayed
// during the run (run.replayedOps), count mismatches are downgraded to "inconclusive"
// instead of "problems": the count comparison cannot distinguish live deletions from
// genuine loss, so it reports honestly rather than raising a false alarm. A quiet
// re-run verifies exactly.
//
// Detection runs on TWO loops: the display tick (MEILI_DRAIN_POLL_MS) renders the
// progress bar from backlog snapshots, and a fast sentinel (MEILI_DRAIN_SENTINEL_MS,
// ramping x2) polls only the run's last task, so short runs finish within ~half a
// second of the real drain instead of freezing a mid-drain frame for a full poll
// interval. The sentinel SELF-RETIRES once its ramp reaches the display cadence —
// from then on the tick is the sole detector. finish() is idempotent — whichever
// loop observes the terminal state first wins.

// Pure helper (unit-tested): derive the progress view from this run's task count, the
// global backlog at run start, and the current global backlog. Live writes during the
// drain (hooks) add to the backlog without adding to `enqueued`, which only makes the
// bar trail slightly — never wrong in the "done" direction.
function computeDrainProgress(enqueued, baseBacklog, currentBacklog) {
	const remaining = Math.max(0, (currentBacklog || 0) - (baseBacklog || 0));
	const current = Math.max(0, Math.min(enqueued, enqueued - remaining));
	return {
		current,
		total: enqueued,
		remaining,
		drained: remaining === 0,
	};
}

async function getBacklogTotal(plugin) {
	const res = await plugin.client.tasks.getTasks({ statuses: ['enqueued', 'processing'], limit: 1 });
	return res && typeof res.total === 'number' ? res.total : 0;
}

// null = Meilisearch unreachable (poll tick aborted; retry on next tick).
async function safeBacklogTotal(plugin) {
	try {
		return await getBacklogTotal(plugin);
	} catch (err) {
		winston.warn(`[plugin/meilisearch] Could not read Meilisearch task backlog: ${err.message}`);
		return null;
	}
}

// B4 hardening — exact drain detection. Meilisearch processes tasks strictly in uid
// order (autobatching merges contiguous ranges but never reorders), so the run's LAST
// task reaching a terminal status guarantees every task with a lower uid is finished
// too. Unlike the backlog-delta formula this cannot fire early when pre-existing
// foreign tasks (the base backlog) complete mid-run and drag the global backlog below
// its starting level. Returns null when the status is genuinely unknown (transient
// error — retry next tick).
async function safeGetLastTask(plugin, lastTaskUid) {
	if (!Number.isFinite(lastTaskUid)) {
		return null;
	}
	try {
		return await plugin.client.tasks.getTask(lastTaskUid);
	} catch (err) {
		const status = err && err.response && err.response.status;
		const code = (err && err.cause && err.cause.code) || (err && err.code);
		if (status === 404 || code === 'task_not_found') {
			// Purged from Meilisearch's ~24h task history — it finished long ago.
			return { status: 'succeeded' };
		}
		winston.warn(`[plugin/meilisearch] Could not read task ${lastTaskUid}: ${err.message}`);
		return null;
	}
}

const TERMINAL_TASK_STATUS = new Set(['succeeded', 'failed', 'canceled']);

// Snapshot the pre-existing backlog so foreign tasks (snapshots, settings from other
// instances) are subtracted out of the progress math.
async function snapshotBacklog(plugin) {
	const total = await safeBacklogTotal(plugin);
	return total === null ? 0 : total;
}

// M6(b): only failures of the task types this run actually enqueues (document
// writes via sendDocuments) count against the run. Failed tasks of OTHER types
// whose uids merely fall inside the run's [first, last] window — e.g.
// indexCreation tasks that lost the concurrent-creation race at boot with
// "Index already exists" — are not this run's failures and must not trip the
// verification. Settings-task failures are already surfaced synchronously by
// runReindex's own waitForSucceededTask calls.
const RUN_TASK_TYPES = new Set(['documentAdditionOrUpdate', 'documentDeletion']);

async function fetchFailedTasks(plugin, run) {
	const failed = [];
	try {
		// `from` anchors descending pagination at the newest uid of our range; the
		// client-side filter keeps only this run's tasks. Over 100 failures in one run
		// the list is truncated (count still exact from the first page) — pathological
		// enough that reporting the first page is sufficient.
		const res = await plugin.client.tasks.getTasks({
			statuses: ['failed'],
			limit: 100,
			from: Number.isFinite(run.lastTaskUid) ? run.lastTaskUid : undefined,
		});
		for (const task of (res && res.results) || []) {
			if (task.uid >= run.firstTaskUid && task.uid <= run.lastTaskUid && RUN_TASK_TYPES.has(task.type)) {
				failed.push({
					uid: task.uid,
					indexUid: task.indexUid,
					type: task.type,
					error: (task.error && task.error.message) || `task ${task.status}`,
				});
			}
		}
	} catch (err) {
		winston.warn(`[plugin/meilisearch] Verification could not list failed tasks: ${err.message}`);
	}
	return failed;
}

// Each verification problem is carried as a translation key + args (rendered per
// admin in their own language — lib/admin.js translates summaryKey on the progress
// bar and on the alert toast; lib/routes.js translates it server-side for the ACP
// result card) plus the English text as the data/log fallback. Language files use
// %1/%2... placeholders.
function problem(key, args, text) {
	return { key: `meilisearch:admin.${key}`, args, text };
}

async function verifyRun(plugin, run) {
	const verification = {
		status: 'verified',
		isVerified: true,
		checkedAt: Date.now(),
		failedTaskCount: 0,
		missingDocs: {},
		extraDocs: {},
		pendingOps: 0,
		quarantined: run.quarantined || 0,
		replayedOps: run.replayedOps || 0,
		problems: [],
	};

	// Pending-queue writes awaiting replay would make count checks cry wolf — report
	// them as a problem instead of a false "missing documents" verdict.
	try {
		verification.pendingOps = (await db.sortedSetCard(PENDING_KEY)) || 0;
	} catch (err) {
		verification.pendingOps = -1;
	}

	const failed = await fetchFailedTasks(plugin, run);
	verification.failedTaskCount = failed.length;
	if (failed.length) {
		verification.failedTasks = failed.slice(0, 10);
		verification.problems.push(problem('verifyFailedTasks',
			[failed.length, failed[0].indexUid || '?', failed[0].uid, failed[0].error],
			`${failed.length} Meilisearch task(s) from this run failed (first: ${failed[0].indexUid || '?'} uid ${failed[0].uid}: ${failed[0].error}).`));
	}

	// Count reconciliation per index — only meaningful for runs that finished Phase 1;
	// a failed run's partial counts would read as massive "missing documents".
	if (run.expected) {
		for (const indexUid of INDEX_UIDS) {
			const expected = run.expected[indexUid];
			if (!Number.isFinite(expected)) continue;
			try {
				const stats = await plugin.client.index(indexUid).getStats();
				const actual = stats.numberOfDocuments || 0;
				const delta = actual - expected;
				if (delta < 0) {
					const missing = -delta;
					if (verification.replayedOps > 0) {
						// B3: live writes were deferred during this reindex and replayed
						// after Phase 1 (deletions among them). Whether each deleted doc
						// had already been indexed by this run (read before the delete)
						// or was skipped (read after) varies per doc, so the count
						// comparison cannot distinguish live deletions from genuine
						// loss. Report honestly instead of raising a false "missing
						// documents" alarm — a quiet re-run verifies exactly.
						verification.problems.push(problem('verifyMissingDocsReplayed',
							[indexUid, missing, actual, expected, verification.replayedOps],
							`Index "${indexUid}": ${missing} document(s) missing (${actual} indexed, ${expected} expected) — ${verification.replayedOps} live write op(s) were replayed during this run, so counts are unreliable. Re-run at a quiet time to verify.`));
					} else {
						verification.missingDocs[indexUid] = missing;
						verification.problems.push(problem('verifyMissingDocs',
							[indexUid, missing, actual, expected],
							`Index "${indexUid}": ${missing} document(s) missing (${actual} indexed, ${expected} expected).`));
					}
				} else if (delta > 0) {
					verification.extraDocs[indexUid] = delta;
					if (verification.replayedOps > 0) {
						verification.problems.push(problem('verifyExtraDocsReplayed',
							[indexUid, delta, actual, expected],
							`Index "${indexUid}": ${delta} extra document(s) (${actual} indexed, ${expected} expected) — live writes were replayed during this run (e.g. new posts), so a small surplus is expected.`));
					} else {
						verification.problems.push(problem('verifyExtraDocsStale',
							[indexUid, delta, actual, expected],
							`Index "${indexUid}": ${delta} extra document(s) (${actual} indexed, ${expected} expected) — possibly stale documents left over from earlier failures.`));
					}
				}
			} catch (err) {
				verification.problems.push(problem('verifyCountCheckFailed',
					[indexUid, err.message],
					`Index "${indexUid}": count check failed (${err.message}).`));
			}
		}
	} else if (run.expected === null) {
		verification.problems.push(problem('verifyRunUnfinished', [],
			'Run did not finish — document count checks were skipped.'));
	}

	if (verification.quarantined) {
		verification.problems.push(problem('verifyQuarantined', [verification.quarantined],
			`${verification.quarantined} document(s) quarantined (single item exceeded the Meilisearch payload limit).`));
	}
	if (verification.pendingOps > 0) {
		verification.problems.push(problem('verifyPendingOps', [verification.pendingOps],
			`${verification.pendingOps} write operation(s) still queued for replay — counts may be incomplete until they are applied.`));
	}

	verification.status = (verification.failedTaskCount > 0 || Object.keys(verification.missingDocs).length) ? 'problems'
		: (verification.problems.length ? 'inconclusive' : 'verified');
	verification.isVerified = verification.status === 'verified';
	verification.summaryKey = verification.problems.length ? verification.problems[0].key : null;
	verification.summaryArgs = verification.problems.length ? verification.problems[0].args : [];
	verification.summary = verification.problems.length
		? verification.problems[0].text + (verification.problems.length > 1 ? ` (+${verification.problems.length - 1} more)` : '')
		: null;
	return verification;
}

async function persistVerification(plugin, verification) {
	try {
		// F8: JSON round-trip (Redis stringifies nested objects — the raw read+write
		// here threw a strict-mode TypeError on the stringified value)
		const existing = await getJsonSetting(plugin, 'lastReindexResult', {});
		existing.verification = verification;
		await setJsonSetting(plugin, 'lastReindexResult', existing);
	} catch (err) {
		winston.error(`[plugin/meilisearch] Could not persist drain verification: ${err.message}`);
	}
}

function publishProgress(plugin) {
	pubsub.publish('meilisearch:reindex', plugin.indexing);
}

function stopDrainMonitor(plugin) {
	if (plugin.drainMonitor) {
		plugin.drainMonitor.stop();
		plugin.drainMonitor = null;
	}
}

function startDrainMonitor(plugin, run) {
	stopDrainMonitor(plugin);
	const startedAt = Date.now();
	let stopped = false;
	let finished = false;
	let timer = null;
	// Sentinel loop state (see sentinelTick below): current ramp delay + pending timer.
	let sentinelTimer = null;
	let sentinelDelay = MEILI_DRAIN_SENTINEL_MS;
	const emit = _.throttle(() => publishProgress(plugin), 500, { leading: true, trailing: true });

	const stop = function () {
		stopped = true;
		if (timer) {
			clearInterval(timer);
		}
		if (sentinelTimer) {
			clearTimeout(sentinelTimer);
		}
		emit.flush();
	};

	// Idempotent: the display tick and the sentinel can observe the same terminal state
	// concurrently (each passes its own `stopped` check before either calls stop()), so
	// the flag — set synchronously before any await — makes the second caller a no-op
	// instead of double-verifying, double-publishing and double-alerting.
	const finish = async function (outcome) {
		if (stopped || finished) return;
		finished = true;
		// Drop any pending throttled display publish BEFORE stop() flushes it: the
		// trailing would otherwise re-emit the last tick's frame (mid-drain on the
		// sentinel/timeout paths, since neither writes meili_progress before
		// finishing) milliseconds before finish's own final publish. lodash
		// cancel() makes the stop() flush a no-op, so finish publishes exactly
		// once; nothing is lost — the final publish supersedes whatever the
		// trailing carried.
		emit.cancel();
		stop();
		if (outcome === 'timeout') {
			winston.warn(`[plugin/meilisearch] Gave up monitoring the Meilisearch task queue after ${Math.round((Date.now() - startedAt) / 60000)} min — still busy. Verification skipped; check Meilisearch's /tasks if concerned.`);
			plugin.indexing.meili_progress = { ...(plugin.indexing.meili_progress || {}), drained: false, timedOut: true };
			publishProgress(plugin);
			return;
		}
		const verification = await verifyRun(plugin, run);
		plugin.indexing.meili_progress = {
			current: run.enqueued,
			total: run.enqueued,
			drained: true,
			verification,
		};
		publishProgress(plugin);
		await persistVerification(plugin, verification);
		if (verification.status === 'problems') {
			winston.error(`[plugin/meilisearch] Reindex verification found problems: ${verification.summary}`);
			if (typeof plugin.notifyAdmins === 'function') {
				plugin.notifyAdmins('reindex:verification', {
					type: 'danger',
					titleKey: '[[meilisearch:admin.reindexVerifyFailed]]',
					messageKey: verification.summaryKey,
					messageArgs: verification.summaryArgs,
					message: verification.summary || '',
				});
			}
		} else if (verification.status === 'verified') {
			winston.info('[plugin/meilisearch] Meilisearch drain complete — post-index verification passed');
		} else {
			winston.warn(`[plugin/meilisearch] Meilisearch drain complete — verification inconclusive: ${verification.summary}`);
		}
	};

	const tick = async function () {
		if (stopped) return;
		const [backlog, lastTask] = await Promise.all([
			safeBacklogTotal(plugin),
			safeGetLastTask(plugin, run.lastTaskUid),
		]);
		if (stopped) return; // stop() may have raced these in-flight polls
		// Drain detection: primary = the run's last task is terminal (exact, see
		// safeGetLastTask); fallback = backlog-delta heuristic when the task status is
		// unknown (never recorded / transient read error). The progress bar stays on
		// the backlog-delta math in both cases — it is approximate but monotonic.
		let drained;
		if (lastTask !== null) {
			drained = TERMINAL_TASK_STATUS.has(lastTask.status);
		} else {
			drained = backlog === null ? false : computeDrainProgress(run.enqueued, run.baseBacklog, backlog).drained;
		}
		if (backlog !== null) {
			const progress = computeDrainProgress(run.enqueued, run.baseBacklog, backlog);
			plugin.indexing.meili_progress = {
				current: progress.current,
				total: progress.total,
				drained,
			};
			emit();
		} else {
			plugin.indexing.meili_progress = {
				...(plugin.indexing.meili_progress || {}),
				drained,
			};
			emit();
		}
		if (drained) {
			await finish('drained');
			return;
		}
		if (Date.now() - startedAt >= MEILI_DRAIN_TIMEOUT_MS) {
			await finish('timeout');
		}
	};

	// Fast drain-detection sentinel. The display tick above renders the bar but also
	// gated drain detection at MEILI_DRAIN_POLL_MS, so a short run whose first tick
	// landed mid-drain (e.g. 1/5) froze that frame on screen for a full interval even
	// though Meilisearch drained the queue in ~100ms. This loop polls ONLY the run's
	// last task (1 GET) starting at MEILI_DRAIN_SENTINEL_MS, ramping x2, and fires
	// finish('drained') the moment the sentinel task is terminal — finish publishes
	// unthrottled, so the bar snaps to enqueued/enqueued ✓ without touching the
	// display cadence or socket chatter. When the task status is unknown (non-finite
	// uid / transient read error) it falls back to the tick's backlog-delta heuristic
	// (1 extra GET on the same ramp); errors back off along the same ramp. Once the
	// next delay would reach MEILI_DRAIN_POLL_MS the sentinel SELF-RETIRES (see the
	// retire check below): at the display cadence its 500ms phase offset buys at most
	// ~500ms in one sub-window per 3s cycle — not worth a GET per tick, and doubled
	// warn chatter on an unreachable Meilisearch, persisting for the whole drain.
	// The tick remains the guaranteed detector before, during and after the
	// sentinel's ~3.5s lifetime, and owns the MEILI_DRAIN_TIMEOUT_MS give-up, so the
	// sentinel needs no timeout of its own.
	const sentinelTick = async function () {
		if (stopped) return;
		let drained = false;
		try {
			const lastTask = await safeGetLastTask(plugin, run.lastTaskUid);
			if (stopped) return;
			if (lastTask !== null) {
				drained = TERMINAL_TASK_STATUS.has(lastTask.status);
			} else {
				const backlog = await safeBacklogTotal(plugin);
				if (stopped) return;
				drained = backlog === null ? false : computeDrainProgress(run.enqueued, run.baseBacklog, backlog).drained;
			}
		} catch (err) {
			// Both reads resolve null on failure; an unexpected throw is treated like a
			// failed read — log and back off rather than silently killing the sentinel.
			winston.warn(`[plugin/meilisearch] Drain sentinel read failed: ${err.message}`);
		}
		if (drained) {
			await finish('drained');
			return;
		}
		// Self-retire once the NEXT delay would reach the display cadence: at the
		// tick's cadence the sentinel's 500ms phase offset buys at most ~500ms in one
		// sub-window per 3s cycle, while its cost (+1 GET per 3s; doubled warn
		// chatter on an unreachable Meilisearch) would persist for the whole drain.
		// Retire = simply stop rescheduling — it must NOT touch
		// stopped/timers/plugin.drainMonitor (it is not stop(): the tick keeps
		// rendering, detecting and owning the give-up timeout). Bounds the
		// sentinel's total cost at ~3-4 GETs per run. Also guards the
		// MEILI_DRAIN_SENTINEL_MS < MEILI_DRAIN_POLL_MS invariant: an inverted
		// pair just retires after the first tick.
		if (sentinelDelay * 2 >= MEILI_DRAIN_POLL_MS) {
			return;
		}
		sentinelDelay *= 2;
		scheduleSentinel();
	};

	const scheduleSentinel = function () {
		if (stopped) return;
		// Positivity guard for the constant: MEILI_DRAIN_SENTINEL_MS = 0 (or
		// negative/NaN) would otherwise hot-loop — the retire check can never trip
		// (0*2 never reaches the poll cadence) and setTimeout clamps to a ~1ms
		// floor, leaving a probe flood paced only by HTTP RTT. A non-positive delay
		// here means "sentinel disabled": zero probes, tick-only behavior (exactly
		// the pre-sentinel build). (!sentinelDelay alone misses negatives, which
		// setTimeout also clamps to 0.)
		if (!(sentinelDelay > 0)) return;
		sentinelTimer = setTimeout(() => {
			sentinelTick().catch(err => winston.error(`[plugin/meilisearch] drain sentinel failed: ${err.message}`));
		}, sentinelDelay);
		if (sentinelTimer.unref) {
			sentinelTimer.unref();
		}
	};

	plugin.drainMonitor = { stop };
	timer = setInterval(() => {
		tick().catch(err => winston.error(`[plugin/meilisearch] drain monitor tick failed: ${err.message}`));
	}, MEILI_DRAIN_POLL_MS);
	if (timer.unref) {
		timer.unref();
	}
	tick().catch(err => winston.error(`[plugin/meilisearch] drain monitor tick failed: ${err.message}`));
	scheduleSentinel();
}

module.exports = {
	computeDrainProgress,
	snapshotBacklog,
	startDrainMonitor,
	stopDrainMonitor,
	verifyRun,
};
