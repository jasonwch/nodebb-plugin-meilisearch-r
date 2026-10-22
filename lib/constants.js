'use strict';

module.exports = {
	REINDEX_BATCH_SIZE: 2000,
	PENDING_KEY: 'plugin:meilisearch:pending',
	// Raised for cluster deferral: every instance's live writes during a reindex
	// park here (transient — drained within minutes of the run finishing).
	PENDING_MAX: 500000,
	DRAIN_MAX: 1000,
	// Cross-instance drain lock (lib/pending-queue.js): serializes batch claiming from
	// the shared pending queue so two instances' drains cannot fetch-and-replay the
	// same batch (duplicate write load; with timing skew, FIFO inversion). Check-then-
	// set like the reindex lock — a duplicate claim in the tiny acquire window is
	// benign (the remove makes the duplicate a no-op for the queue, replays are
	// idempotent), so unlike the reindex lock it doesn't need owner-token strictness.
	// Held per batch and refreshed per replayed op; a dead holder self-expires within
	// the TTL below.
	DRAIN_LOCK_KEY: 'plugin:meilisearch:drain:lock',
	DRAIN_LOCK_TTL_SECONDS: 60,
	// How long drainAll waits between rounds when another instance holds the drain
	// lock before re-checking (the lock is being productive elsewhere — wait it out
	// rather than no-progress-bail; the drain time cap still bounds the total).
	// Doubles up to DRAIN_LOCK_WAIT_MAX_MS (exponential backoff — the wait is idle
	// time, but the retry's lock-ttl reads shouldn't chatter).
	DRAIN_LOCK_RETRY_MS: 200,
	DRAIN_LOCK_WAIT_MAX_MS: 2000,
	// Monotonic sequence backing park scores. Date.now() scores tie within the same
	// millisecond (tie-break then falls to member bytes — wrong order possible) and
	// invert under cross-instance clock skew (a fast-clock instance's older op sorts
	// last and overwrites newer content on replay). db.increment is atomic on all
	// adapters (redis INCR / mongo $inc / postgres ::NUMERIC + 1), so scores are
	// strictly increasing cluster-wide regardless of node clocks.
	//
	// PARK_SCORE_FLOOR is added to the raw counter IN JAVASCRIPT (lib/pending-queue.js
	// nextParkScore) — never written to the DB: seeding the counter key via db.set
	// stores a STRING on MongoDB, whose $inc then throws on that field permanently.
	// The floor keeps new scores above any legacy Date.now()-scored members still
	// queued at deploy time; deploy with the queue empty (no run in flight) so the
	// two never mix meaningfully.
	PENDING_SEQ_KEY: 'plugin:meilisearch:pending:seq',
	PARK_SCORE_FLOOR: 1e15,
	// H2: warn (throttled) when a single parked member is unusually large. After
	// the id-only park changes members are ~KB; a member this big means some op
	// is parking full content — the database may reject it outright (Mongo's 16MB
	// BSON document cap, losing the deferral) and every adapter pays a proportional
	// JSON.parse at drain time. Warn-only by design: chunking arbitrary op
	// payloads is not safely implementable (ops like onTopicMerge are not
	// splittable), and the id-only parks keep every known op far below this.
	PENDING_MEMBER_WARN_BYTES: 1024 * 1024,
	REINDEX_LOCK_KEY: 'plugin:meilisearch:reindex:lock',
	REINDEX_LOCK_TTL: 3600,
	LOCK_REFRESH_INTERVAL: 5 * 60 * 1000,
	REPLAY_OPS: [
		'indexPost', 'deindexPost', 'indexTopic', 'deindexTopic', 'deindexPostsPurge',
		'deindexTopicsPurge', 'reindexTopicPosts', 'deindexTopicPosts', 'restoreTopic',
		'changePostOwner', 'changeTopicOwner', 'onTopicMerge', 'onScheduledPublish',
		'indexMessage', 'deindexMessage',
	],
	GLOBAL_CHAT_SEARCH_LIMIT: 200,
	GLOBAL_CHAT_SEARCH_FETCH: 300,
	MEMBERSHIP_CHUNK_SIZE: 100,
	DECORATION_BATCH_SIZE: 10,
	// Sync wait window for plugin.updateEmbedders' Meilisearch task before falling back to
	// background poller. 60s keeps ACP save responsive; if the embedder task is still
	// running server-side (re-embedding every doc legitimately exceeds 60s on a forum of
	// any size), updateEmbedders persists the fingerprint optimistically and startEmbedderPoller
	// verifies the eventual outcome.
	EMBEDDER_TASK_TIMEOUT_MS: 60000,
	// Background poller ceiling (60 min) and interval (5 s) for verifying embedder tasks
	// that exceeded the sync wait window. 60 min covers re-embedding up to ~1M posts at
	// OpenAI Tier 1 rate limits (1M TPM). For forums larger than that, the poller gives up
	// and relies on reattachPollers to reconcile the task outcome on the next restart
	// (within Meilisearch's ~24h task retention window). No double-paid-API-cost risk —
	// fingerprint is preserved, not cleared, on poller timeout (see startEmbedderPoller
	// MeilisearchTaskTimeOutError handling in lib/client.js).
	EMBEDDER_TASK_BG_TIMEOUT_MS: 3600000,
	EMBEDDER_TASK_BG_INTERVAL_MS: 5000,
	// Meilisearch SDK client-side HTTP timeout. Caps every fetch() the SDK makes (including
	// the initial enqueue POST for embedder tasks) at 30s — without this, the SDK uses no
	// AbortSignal (node_modules/meilisearch/dist/index.js:340), so an unreachable Meilisearch
	// host can leave plugin.updateEmbedders (and therefore the ACP Save button) stuck
	// indefinitely.
	MEILI_HTTP_TIMEOUT_MS: 30000,
	// sendDocuments hardening (lib/send-documents.js): optimistic full-batch POSTs with
	// transient-error retry and 413 payload bisection. Batches always START at the full
	// size; only N consecutive batches needing bisection shrink the start size for the
	// remainder of the run (in-memory, per-run — never persisted, so every run
	// re-calibrates from scratch).
	MEILI_SEND_RETRIES: 2,
	MEILI_SEND_RETRY_BASE_MS: 250,
	MEILI_BISECT_CONSECUTIVE: 5,
	// Meilisearch-side drain monitor + post-drain verification (lib/drain.js). The poll
	// only reads the task-queue backlog (1 GET per tick); the ceiling exists so a
	// permanently-busy/unreachable Meilisearch cannot leak the interval forever.
	MEILI_DRAIN_POLL_MS: 3000,
	MEILI_DRAIN_TIMEOUT_MS: 16 * 60 * 60 * 1000,
	// Cluster deferral: how long a pubsub-synced "reindex running" state stays
	// trusted without a progress publish before instances self-heal (the reindexing
	// instance died mid-run). The reindex runs a whole-run keepalive every
	// REINDEX_KEEPALIVE_MS, so a healthy run never approaches this threshold no
	// matter how silent its stretches are (awaited one-time setup tasks each carry
	// a 600s timeout; a pathological batch can take minutes). The margin above the
	// keepalive covers pubsub delivery hiccups.
	INDEXING_STALE_MS: 15 * 60 * 1000,
	// Whole-run keepalive interval: refreshes the synced running state through
	// silent stretches so other instances — and this one, via its own live
	// traffic — never falsely self-heal mid-run (stop deferring, flip local
	// running=false, kick drainAlls into the run).
	REINDEX_KEEPALIVE_MS: 30 * 1000,
	// DB-backed reindex liveness heartbeat (lib/reindex.js writes it every keepalive
	// tick and at acquisition; lib/lock.js and lib/pending-queue.js read it).
	// Closes two gaps:
	//   1. Dead-holder reclaim: the lock's 5-min refresh RESETS the TTL, so a crash
	//      leaves up to REINDEX_LOCK_TTL of blockage behind. With the heartbeat,
	//      acquireReindexLock can tell a dead holder (stale heartbeat) from a live
	//      one and steal the lock ~90s after death — corruption-safe via the
	//      owner-token checks (the dead holder's refresh/release no-op).
	//   2. False self-heal: a >= INDEXING_STALE_MS pubsub outage during a LIVE run
	//      must not make peers resume direct writes mid-run. When the pubsub-synced
	//      state looks stale, the heartbeat is the ground truth (DB reads still
	//      work when pubsub doesn't, on every adapter).
	// 3x the keepalive: a live holder writes every 30s, so 90s of silence means the
	// holder is dead or its event loop is stalled past recovery. A lock with NO
	// heartbeat is treated as live (a pre-heartbeat-version holder — conservative).
	REINDEX_HEARTBEAT_KEY: 'plugin:meilisearch:reindex:heartbeat',
	REINDEX_HEARTBEAT_STALE_MS: 90 * 1000,
	// Total-time cap for the reindex finally-block's drain-until-empty of the shared
	// pending queue; anything left over falls back to health-tick drains (1000/tick).
	REINDEX_DRAIN_TIMEOUT_MS: 60 * 60 * 1000,
};
