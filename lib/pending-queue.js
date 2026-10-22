'use strict';

const winston = nodebb.require('winston');
const db = nodebb.require('./src/database');
const nconf = nodebb.require('nconf');
const { AsyncLocalStorage } = require('node:async_hooks');
const {
	PENDING_KEY, PENDING_MAX, DRAIN_MAX, REPLAY_OPS, INDEXING_STALE_MS, REINDEX_DRAIN_TIMEOUT_MS,
	DRAIN_LOCK_KEY, DRAIN_LOCK_TTL_SECONDS, DRAIN_LOCK_RETRY_MS, DRAIN_LOCK_WAIT_MAX_MS, PENDING_SEQ_KEY, PARK_SCORE_FLOOR,
	PENDING_MEMBER_WARN_BYTES,
} = require('./constants');
const { replayAdds, replayDeletes } = require('./replay');
const { isReindexHeartbeatFresh } = require('./lock');

// Replay context: while a drain invokes the plugin's write hooks, those calls must
// bypass maybeDefer's park logic. Two reasons:
//   1. The reindex finally-block now drains BEFORE publishing running=false (flaw 1
//      fix), so a replayed op would otherwise be re-parked by the still-active run
//      state — re-parked at the BACK of the queue with a fresh score, the drain
//      would livelock to its time cap replaying the same ops forever.
//   2. Parked-vs-direct ordering is exactly what that park would corrupt.
// AsyncLocalStorage keeps the marker on the replay's own async chain only: live
// traffic arriving during the drain originates from unrelated async contexts
// (socket dispatch) and still parks, preserving strict FIFO against the replays.
const replayContext = new AsyncLocalStorage();
function runReplay(fn) { return replayContext.run({}, fn); }
function isReplayOp() { return replayContext.getStore() !== undefined; }

// PENDING_MAX eviction warning, throttled to one per minute so a prolonged outage
// sitting at the cap doesn't flood the log (each enqueue drops one oldest op).
let lastEvictionWarnAt = 0;

// H2: oversized-member warning, throttled the same way (see
// PENDING_MEMBER_WARN_BYTES in constants.js).
let lastSizeWarnAt = 0;

// Monotonic park score (flaw 7 fix): strictly increasing cluster-wide via the
// adapter's native atomic counter — db.increment (redis INCR / mongo $inc /
// postgres ::NUMERIC + 1) — immune to same-millisecond ties and cross-instance
// clock skew.
//
// The floor is added IN JAVASCRIPT, never written to the database. An earlier
// revision seeded the counter key itself via db.set(PENDING_SEQ_KEY, ...) when the
// raw value looked too low — that poisons the key on MongoDB: db.set stores the
// value as a STRING (mongo's setObject $set keeps JS types), and $inc throws
// "Cannot apply $inc to a value of non-numeric type" on a string field, so the
// very first park after the top-up failed and the key stayed broken until
// manually deleted. Redis (INCR parses the string) and postgres (::NUMERIC casts
// it) tolerated it, but only by accident of their own coercions. Keeping the raw
// counter small (1, 2, ...) and adding PARK_SCORE_FLOOR at score-computation time
// is adapter-native everywhere: every write to the counter goes through the same
// atomic increment path that created it. The floor keeps scores above any legacy
// Date.now()-scored members still queued from a pre-deploy run (deploy procedure:
// queue empty, no run in flight).
async function incrementSeqCounter() {
	try {
		return await db.increment(PENDING_SEQ_KEY);
	} catch (err) {
		// Self-heal a counter key poisoned by the ms_1.3_12 db.set() top-up — but
		// ONLY on the positive poison signature: the Mongo adapter AND a
		// STRING-typed stored value. On Mongo, an increment-maintained counter is
		// always a BSON number ($inc upsert creates it numeric), so a string-typed
		// value is un-incrementable by definition — deleting it is the correct
		// eventual action no matter WHY this particular increment failed.
		//
		// The gate is what keeps a TRANSIENT failure on a HEALTHY counter safe
		// (e.g. a mongo primary stepdown: writes fail while reads still succeed):
		// a numeric-typed value (or a read failure) rethrows instead of resetting,
		// because resetting a live counter to 1 would land new scores BELOW
		// already-issued PARK_SCORE_FLOOR+N members still in the queue — the
		// cluster-wide FIFO inversion the sequence exists to prevent.
		//
		// Redis and postgres never develop a poison state (INCR parses strings /
		// ::NUMERIC casts them), and their HEALTHY counters legitimately read back
		// as strings — so for them the heal is structurally unreachable and every
		// increment error rethrows. A parseability check cannot make these
		// distinctions; only adapter + stored type can.
		if (nconf.get('database') !== 'mongo') {
			throw err;
		}
		let current;
		try {
			current = await db.get(PENDING_SEQ_KEY);
		} catch {
			throw err; // reads failing too — a DB-wide problem, not a counter-type problem
		}
		if (typeof current === 'string') {
			try {
				await db.delete(PENDING_SEQ_KEY);
			} catch {
				throw err; // delete failed — the DB is still in trouble; the next park re-attempts the heal
			}
			winston.warn('[plugin/meilisearch] Pending-queue sequence counter is string-typed (poisoned by the ms_1.3_12 db.set() top-up) — resetting it; park scores continue above the JS-side floor');
			return await db.increment(PENDING_SEQ_KEY);
		}
		throw err;
	}
}

async function nextParkScore() {
	const seq = Number(await incrementSeqCounter());
	if (!Number.isFinite(seq)) {
		// An adapter that returned no usable value (not expected on any supported
		// backend) — degrade to wall-clock ordering rather than failing the write.
		// The floor offset keeps the degraded score above seq-scored members.
		return PARK_SCORE_FLOOR + Date.now();
	}
	return PARK_SCORE_FLOOR + seq;
}

// Persists a write that couldn't be applied to MeiliSearch yet, so it can be replayed
// later. One shared, persistent queue for BOTH deferral reasons:
//   - live writes during a reindex, from ANY instance (cluster mode): the indexing
//     state is pubsub-synced, so every instance parks its writes here, and the
//     reindexing instance's finally-block drain replays them cluster-wide AFTER all
//     reindex batches — Meilisearch applies tasks FIFO per index, so the fresh
//     content enqueued last wins over the batches' earlier-read copies
//   - writes that failed while MeiliSearch was unhealthy (health-tick drain)
async function enqueuePending(op, payload) {
	const count = await db.sortedSetCard(PENDING_KEY);
	if (count >= PENDING_MAX) {
		const oldest = await db.getSortedSetRange(PENDING_KEY, 0, 0);
		if (oldest.length) {
			await db.sortedSetRemove(PENDING_KEY, oldest);
			const now = Date.now();
			if (now - lastEvictionWarnAt > 60000) {
				lastEvictionWarnAt = now;
				// Log the dropped op's NAME only — never the member JSON, which can
				// contain document content.
				let droppedOp = op;
				try {
					droppedOp = JSON.parse(oldest[0]).op || op;
				} catch (err) {
					// keep the incoming op name
				}
				winston.warn(`[plugin/meilisearch] Pending queue at capacity (${PENDING_MAX}); dropped the oldest op "${droppedOp}" — writes deferred during a prolonged outage may be lost`);
			}
		}
	}
	const member = JSON.stringify({ op, payload });
	// H2: warn-only size guard (see PENDING_MEMBER_WARN_BYTES). Member size never
	// includes secrets or content in the log line — the op name and byte count only.
	if (member.length >= PENDING_MEMBER_WARN_BYTES) {
		const now = Date.now();
		if (now - lastSizeWarnAt > 60000) {
			lastSizeWarnAt = now;
			winston.warn(`[plugin/meilisearch] Pending-queue member is unusually large (${Math.round(member.length / 1024)} KB, op "${op}") — a database limit (e.g. Mongo's 16MB document cap) may reject it and deferrals may be lost; check that this op parks ids only`);
		}
	}
	await db.sortedSetAdd(PENDING_KEY, await nextParkScore(), member);
}

// A reindex is in flight only while the pubsub-synced indexing state says so AND that
// state is fresh. If the reindexing instance dies mid-run it never publishes the
// completion payload, leaving running=true stuck on every instance forever — the
// staleness window bounds exactly that failure (see maybeDefer's self-heal).
function isReindexActive(plugin) {
	if (!plugin.indexing || !plugin.indexing.running) {
		return false;
	}
	return Date.now() - (plugin.indexingUpdatedAt || 0) < INDEXING_STALE_MS;
}

// Decides whether a write hook should be deferred instead of applied immediately.
async function maybeDefer(plugin, op, payload) {
	if (isReplayOp()) {
		// Queue replay: apply directly. This op was claimed from the FRONT of the
		// queue — its ordering is the queue's FIFO; re-parking it would livelock the
		// drain (see replayContext above).
		return false;
	}
	if (plugin.indexing && plugin.indexing.running && !isReindexActive(plugin)) {
		// The pubsub-synced state looks stale — but the run's liveness is
		// ground-truthed by a DB heartbeat (written by its keepalive). A pubsub
		// outage must NOT flip this instance to direct writes mid-run (the exact
		// stale-overwrite the deferral exists to prevent): only self-heal when the
		// heartbeat is gone or stale too.
		if (await isReindexHeartbeatFresh()) {
			// Run is alive; our pubsub view is the stale one. Adopt the DB truth
			// locally (re-checked at most once per staleness window) and keep
			// deferring — the reindexer's finally drains everything in order.
			plugin.indexingUpdatedAt = Date.now();
			winston.warn('[plugin/meilisearch] Reindex state looks stale locally, but the run\'s DB heartbeat is fresh (pubsub delivery loss?) — staying deferred');
		} else {
			// Stale run state: the reindexing instance most likely died mid-run (no
			// progress publishes AND no heartbeat for > the staleness windows). Self-
			// heal — stop deferring, release the drain guard, and kick a background
			// drain of the orphaned queue so writes already parked there are not
			// lost. Every instance detects this independently on its next write hook.
			winston.warn('[plugin/meilisearch] Reindex state is stale (no progress updates, heartbeat gone) — assuming the run died mid-flight; resuming direct writes');
			plugin.indexing.running = false;
			setImmediate(() => {
				drainAll(plugin).catch(err => winston.error(`[plugin/meilisearch] stale-state drain failed: ${err.message}`));
			});
		}
	}
	if (isReindexActive(plugin)) {
		// A reindex is in flight on some instance (running is pubsub-synced): park the
		// write in the shared persistent queue for the reindexer's finally-block
		// drain. This replaces the old per-instance in-memory buffer, which had no
		// drain path on non-reindexing instances (cluster) and died with the process.
		await enqueuePending(op, payload);
		return true;
	}
	if (!plugin.healthy && !(await plugin.checkHealth())) {
		await enqueuePending(op, payload);
		return true;
	}
	return false;
}

// Runs a MeiliSearch write, falling back to the pending queue on failure.
async function meiliWrite(plugin, op, payload, fn) {
	try {
		await fn();
	} catch (err) {
		winston.error(`[plugin/meilisearch] ${op} write failed: ${err.message}`);
		plugin.healthy = false;
		await enqueuePending(op, payload);
	}
}

// Per-batch cross-instance claim of the pending queue (flaw 2 fix). Held per batch
// (refreshed per replayed op) so a dead holder self-expires within
// DRAIN_LOCK_TTL_SECONDS without a refresh interval. Acquire is check-then-set
// verified by a re-read: two simultaneous acquirers both pass the ttl check, but
// only the LAST setter survives its own verification read — the other backs off
// before fetching any batch. The residual window (a second set landing after the
// winner's verification read) is a full DB round-trip wide at worst and its
// consequence is a benign duplicate replay of an idempotent batch. The owner check
// on release keeps a slow batch from deleting a successor's claim.
async function acquireDrainLock() {
	const ttl = await db.ttl(DRAIN_LOCK_KEY);
	if (ttl > 0) {
		return null;
	}
	const token = `${process.pid}:${Date.now()}:${Math.random().toString(36).slice(2)}`;
	await db.set(DRAIN_LOCK_KEY, token);
	await db.expire(DRAIN_LOCK_KEY, DRAIN_LOCK_TTL_SECONDS);
	const holder = await db.get(DRAIN_LOCK_KEY);
	if (holder !== token) {
		return null; // lost the simultaneous-acquire race — the other instance owns it
	}
	return token;
}

async function releaseDrainLock(token) {
	if (!token) {
		return;
	}
	try {
		const holder = await db.get(DRAIN_LOCK_KEY);
		if (holder === token) {
			await db.delete(DRAIN_LOCK_KEY);
		}
	} catch (err) {
		winston.warn(`[plugin/meilisearch] Could not release the drain lock: ${err.message}`);
	}
}

// Simple parked ops the batch replay path (replayMembers) can fuse: strictly
// id-scoped, single-document writes with NO cascade side effects. Verified
// against lib/hooks/*.js — indexTopic (move: reindexes the topic's posts),
// deindexTopic (deindexes the topic's posts), restoreTopic (reindexes posts) all
// cascade, so topics and every multi-doc op stay on the per-op path.
const SIMPLE_OPS = {
	indexPost: { index: 'post', kind: 'add', idOf: p => p && p.post && p.post.pid },
	deindexPost: { index: 'post', kind: 'delete', idOf: p => p && p.post && p.post.pid },
	indexMessage: { index: 'chat_message', kind: 'add', idOf: p => p && p.message && p.message.mid },
	deindexMessage: { index: 'chat_message', kind: 'delete', idOf: p => p && p.message && p.message.mid },
};

// Replays one claimed batch (members in score order). Simple ops accumulate into
// SEGMENTS separated by complex ops (which replay per-op in score position, so a
// batched write can never jump a complex op that touches the same documents).
//
// DEDUP EQUIVALENCE: within a segment, each document collapses to its LAST op —
// but ONLY across same-kind runs. add/add is idempotent (the bulk path re-fetches
// fresh content, so the surviving add is the same upsert sequential replay would
// end on) and delete/delete is idempotent removal. A KIND TRANSITION (delete→add
// or add→delete) is NOT collapsible: an ADD's apply-path is CONDITIONAL (deleted/
// scheduled/purged-at-replay ⇒ skip — not delete), so sequential replay of
// [delete, filtered-add] REMOVES the document while last-write-wins would keep
// it stale in the index. Documents with kind transitions therefore replay per-op
// through their hooks (ground-truth semantics); only single-kind documents batch
// into one bulk fetch + one sendDocuments per (index, kind).
//
// Returns the set of member strings that were successfully APPLIED — the caller
// removes exactly those from the queue AFTER the batch (at-least-once: a crash
// mid-batch leaves un-applied members queued for the next drain, and re-playing
// an already-applied member is idempotent).
async function replayMembers(plugin, members, retried) {
	const applied = new Set();
	// A failed member stays queued for the next drain session (at-least-once); a
	// SECOND failure in the same session is a poison pill — drop it (bounded churn,
	// matching the old re-park-once-then-drop semantics).
	const markFailed = (member) => {
		if (retried.has(member)) {
			applied.add(member);
		} else {
			retried.add(member);
		}
	};
	const replayPerOp = async (entry) => {
		if (!entry.op || !REPLAY_OPS.includes(entry.op)) {
			applied.add(entry.member); // malformed/unknown member: dropped, as before
			return;
		}
		try {
			await runReplay(() => plugin[entry.op](entry.payload));
			// meiliWrite swallows write failures (re-parks internally, flips
			// healthy=false, does NOT rethrow) — "hook didn't throw" is not
			// "applied". A healthy flip is the failure signal for per-op replays.
			if (plugin.healthy) {
				applied.add(entry.member);
			} else {
				markFailed(entry.member);
			}
		} catch (err) {
			winston.error(`[plugin/meilisearch] replay failed: ${err.message}`);
			markFailed(entry.member);
		}
	};

	const flushSegment = async (buffered) => {
		if (!buffered.length) return;
		// Group the segment's entries per document, in park order
		const byDoc = new Map();
		for (const entry of buffered) {
			const key = `${entry.desc.index}:${entry.id}`;
			if (!byDoc.has(key)) {
				byDoc.set(key, []);
			}
			byDoc.get(key).push(entry);
		}
		const sequential = [];
		const batched = [];
		for (const entries of byDoc.values()) {
			const hasTransition = entries.some((e, i) => i > 0 && e.desc.kind !== entries[i - 1].desc.kind);
			if (hasTransition) {
				sequential.push(...entries);
			} else {
				batched.push(entries[entries.length - 1]);
			}
		}
		// Transition docs: per-op hook replay in score order (see DEDUP EQUIVALENCE)
		for (const entry of sequential) {
			await replayPerOp(entry);
		}
		// Single-kind docs: group by (index, kind), in first-resolved order
		const groups = [];
		const groupOf = new Map();
		for (const entry of batched) {
			const gk = `${entry.desc.index}:${entry.desc.kind}`;
			if (!groupOf.has(gk)) {
				groupOf.set(gk, { index: entry.desc.index, kind: entry.desc.kind, entries: [] });
				groups.push(groupOf.get(gk));
			}
			groupOf.get(gk).entries.push(entry);
		}
		for (let i = 0; i < groups.length; i += 1) {
			const group = groups[i];
			try {
				if (group.kind === 'add') {
					await replayAdds(plugin, group.index, group.entries.map(e => e.id));
				} else {
					await replayDeletes(plugin, group.index, group.entries.map(e => e.id));
				}
			} catch (err) {
				winston.error(`[plugin/meilisearch] replay batch failed (${group.index} ${group.kind}): ${err.message}`);
				plugin.healthy = false;
				// This failed group's entries stay queued (at-least-once, poison-bounded
				// via markFailed); not-yet-executed groups are untouched — they simply
				// stay queued at their original position for the next drain.
				for (const entry of groups[i].entries) {
					markFailed(entry.member);
				}
				return;
			}
			for (const entry of group.entries) {
				applied.add(entry.member);
			}
		}
	};

	let buffered = [];
	for (const member of members) {
		let parsed = null;
		try {
			parsed = JSON.parse(member);
		} catch (err) {
			// malformed member: dropped below, not re-enqueued (same as the per-op path)
		}
		const desc = parsed && SIMPLE_OPS[parsed.op];
		const id = desc ? desc.idOf(parsed.payload) : undefined;
		if (desc && id !== undefined && id !== null) {
			buffered.push({ desc, id, member, op: parsed.op, payload: parsed.payload });
			continue;
		}
		// Complex op, malformed member, or simple op without a usable id: flush the
		// buffered segment first (preserving score order around the complex op),
		// then replay per-op through the hook.
		await flushSegment(buffered);
		buffered = [];
		await replayPerOp({ op: parsed && parsed.op, payload: parsed && parsed.payload, member });
	}
	await flushSegment(buffered);
	return applied;
}

// Keeps a claimed drain lock alive through pathologically slow batch replays. A
// single sendDocuments can run ~91s worst-case (2 retries x the 30s SDK HTTP
// timeout, plus backoff — and 413 bisection stacks more such sequences) — far
// past the 60s claim TTL — and every other refresh point sits BETWEEN sends,
// never inside one. A background owner-checked refresher (every TTL/3) closes all
// of those gaps regardless of what the send path does; a dead holder still
// self-expires within the TTL, and fast batches complete before a single tick
// fires (zero refresh overhead). If the claim was nonetheless lost (expired and
// re-claimed elsewhere), the owner check stops refreshing the successor's lock.
function startDrainClaimRefresher(token) {
	const refresher = setInterval(() => {
		(async () => {
			try {
				const holder = await db.get(DRAIN_LOCK_KEY);
				if (holder !== token) {
					clearInterval(refresher);
					return;
				}
				await db.expire(DRAIN_LOCK_KEY, DRAIN_LOCK_TTL_SECONDS);
			} catch (err) {
				// non-fatal: the TTL self-expires a dead claim anyway
			}
		})();
	}, Math.max(1000, (DRAIN_LOCK_TTL_SECONDS * 1000) / 3));
	if (refresher.unref) {
		refresher.unref();
	}
	return refresher;
}

// Replays queued writes once MeiliSearch is healthy and no reindex is in flight.
// Returns the number of ops replayed. `retried` bounds failure retries across a whole
// drain session (see drainAll): a transiently failing op gets re-enqueued once and
// retried on a later drain; a permanently failing op cannot churn the loop.
//
// In the replay context (drainAll), the reindex-active gate is bypassed: the reindex
// finally-block drains while its run state is still published. `deadlineAt` (ms
// timestamp, optional — drainAll passes its time cap) bounds the loop: without it a
// single drainPending round can run to DRAIN_MAX ops (with slow ops, hours past the
// caller's cap; batch granularity only).
async function drainPending(plugin, retried = new Set(), deadlineAt = null) {
	const replaying = isReplayOp();
	if (!plugin.healthy) {
		return 0;
	}
	if (!replaying && (plugin.reindexingForced || isReindexActive(plugin))) {
		return 0;
	}
	let drained = 0;
	for (;;) {
		if (!plugin.healthy) {
			break;
		}
		if (!replaying && isReindexActive(plugin)) {
			break;
		}
		if (deadlineAt !== null && Date.now() >= deadlineAt) {
			break;
		}
		// Cheap emptiness gate BEFORE claiming the lock: on an idle system every
		// instance's health tick lands here with an empty queue, and the lock
		// acquire below would otherwise churn ~6 write RTs per tick forever for
		// nothing. One read-only card; the actual fetch+remove still happens under
		// the lock.
		if (!(await db.sortedSetCard(PENDING_KEY))) {
			break;
		}
		const lockToken = await acquireDrainLock();
		if (!lockToken) {
			// Another instance owns the queue right now — leave this round to it.
			return drained;
		}
		// Background claim refresher: keeps the claim alive through slow batch
		// replays (see startDrainClaimRefresher). Cleared + released below, no
		// matter how the batch ends.
		const refresher = startDrainClaimRefresher(lockToken);
		let members = [];
		try {
			members = await db.getSortedSetRange(PENDING_KEY, 0, 49);
			if (!members.length) {
				break;
			}
			// AT-LEAST-ONCE (crash safety): replay FIRST, then remove only the members
			// that were successfully applied. The old remove-then-replay order lost up
			// to 50 deferred writes when the process died between the queue mutation
			// and the replay; now a death mid-batch leaves the un-applied members
			// queued for the next drain (replays are idempotent upserts/deletes).
			const applied = await replayMembers(plugin, members, retried);
			if (applied.size) {
				// NOTE: NodeBB's db.sortedSetRemove signature is (key, value|array) on
				// BOTH adapters — a variadic spread here would silently remove only
				// the FIRST member per call (a pre-existing bug this array form fixes).
				await db.sortedSetRemove(PENDING_KEY, [...applied]);
			}
			drained += members.length;
		} finally {
			clearInterval(refresher);
			await releaseDrainLock(lockToken);
		}
		if (drained >= DRAIN_MAX) {
			break;
		}
	}
	return drained;
}

// Drains the shared pending queue until empty (or the time cap / a no-progress bail).
//
// Used by the reindex finally-block — which now runs BEFORE the completion publish
// (flaw 1 fix): running=true is still published here, so every other instance keeps
// parking live writes (nothing can land between the run's batches and the parked
// replays — a parked delete can no longer land after a post-run direct re-add) and
// their health-tick drains stay gated by isReindexActive. The whole drain runs in
// the replay context (drainPending's guards bypassed, replayed ops applied
// directly), and the whole-run keepalive + lock refresh in reindex.js stay live
// through it so no instance falsely self-heals mid-drain. Also used by the stale-run
// self-heal in maybeDefer (running=false already flipped — the bypass is then a
// no-op). Health ticks (drainPending, 1000 ops each) are the backstop for anything
// left behind. Returns the number of ops replayed.
async function drainAll(plugin, options = {}) {
	const timeoutMs = Number.isFinite(options.timeoutMs) ? options.timeoutMs : REINDEX_DRAIN_TIMEOUT_MS;
	const retried = new Set();
	const startedAt = Date.now();
	let drained = 0;
	let lockWaitMs = DRAIN_LOCK_RETRY_MS; // exponential backoff while another instance drains
	for (;;) {
		if (!plugin.healthy) {
			break;
		}
		const before = await db.sortedSetCard(PENDING_KEY);
		if (!before) {
			break;
		}
		if (Date.now() - startedAt >= timeoutMs) {
			winston.warn(`[plugin/meilisearch] Pending-queue drain hit its time cap; ${before} op(s) left for health-tick drains`);
			break;
		}
		drained += await runReplay(() => drainPending(plugin, retried, startedAt + timeoutMs));
		const after = await db.sortedSetCard(PENDING_KEY);
		if (after >= before) {
			// Zero-progress round. If another instance simply holds the drain lock
			// (its batch replay is in flight there), the queue IS being drained —
			// wait it out instead of bailing. The wait backs off exponentially (the
			// retry's lock checks shouldn't chatter) and the time cap above still
			// bounds the total wait if that holder dies.
			if ((await db.ttl(DRAIN_LOCK_KEY)) > 0) {
				await new Promise(resolve => setTimeout(resolve, lockWaitMs));
				lockWaitMs = Math.min(lockWaitMs * 2, DRAIN_LOCK_WAIT_MAX_MS);
				continue;
			}
			// Genuine no progress: either replayed ops are failing and being
			// re-enqueued, or a drain guard flipped mid-drain (reindex started,
			// forced-run flag, phantom run publish) so this iteration replayed
			// nothing against a non-empty queue. Leave the rest to health-tick
			// drains instead of spinning until the time cap.
			winston.warn(`[plugin/meilisearch] Pending-queue drain made no net progress; ${after} op(s) left for health-tick drains`);
			break;
		}
		lockWaitMs = DRAIN_LOCK_RETRY_MS; // progress — reset the backoff
	}
	return drained;
}

module.exports = { enqueuePending, maybeDefer, meiliWrite, drainPending, drainAll, isReindexActive };
