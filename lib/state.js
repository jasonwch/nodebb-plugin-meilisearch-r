'use strict';

const winston = nodebb.require('winston');
const Sockets = nodebb.require('./src/socket.io');
const pubsub = nodebb.require('./src/pubsub');
const user = nodebb.require('./src/user');
const { defaults, breakingSettings } = require('./defaults');

// Sockets currently joined to 'admin/plugins/meilisearch' (set by app.enterRoom in ACP).
// NodeBB 4.15.0 SocketMeta.rooms.enter only guards uid_/chat_/topic_/category_ prefixes
// (verified in NodeBB v4.15.0 src/socket.io/meta.js) — arbitrary room names like
// 'admin/plugins/meilisearch' are freely joinable by any authenticated user. Filter by
// isAdministrator before emitting, so alert payloads (which may contain embedder endpoint
// URLs / model names from Meilisearch task error messages) don't reach authenticated
// non-admins who manually joined the room. isAdministrator is Redis-cached for 60s by
// NodeBB core (src/user.js), so for low-frequency alert emissions the overhead is sub-ms.
async function emitToAdmins(eventName, payload) {
	// M5: cheap local pre-check — skip the cluster-wide fetchSockets() round-trip
	// when no ACP socket joined the alert room on THIS instance. Every instance's
	// pubsub handler runs on every progress tick, so without this each of N
	// instances paid an O(N) adapter exchange per tick (~N² cluster-wide) even
	// with zero admins watching. Delivery is preserved: the instance actually
	// holding an admin's socket always sees the room in its local adapter Map
	// (socket.join is local-first — the same pattern NodeBB core uses in
	// src/socket.io/index.js). Worst case (adapter semantics change) this
	// degrades to exactly the previous behavior.
	const io = Sockets.server;
	const localRoom = (io && io.sockets && io.sockets.adapter && io.sockets.adapter.rooms)
		? io.sockets.adapter.rooms.get('admin/plugins/meilisearch')
		: null;
	if (!localRoom || !localRoom.size) return;
	const sockets = await Sockets.server.in('admin/plugins/meilisearch').fetchSockets();
	if (!sockets.length) return;
	const adminChecks = await Promise.all(
		// Use s.data.uid (cluster-safe — socket.data is shared across nodes via fetchSockets,
		// verified in NodeBB v4.15.0 src/socket.io/index.js:87-88). s.uid only exists on the
		// local node, so in a multi-node cluster, s.uid would be undefined for remote sockets,
		// silently defeating the admin-only filter.
		sockets.map(s => (s.data && s.data.uid ? user.isAdministrator(s.data.uid) : Promise.resolve(false))),
	);
	sockets.forEach((s, i) => {
		if (adminChecks[i]) s.emit(eventName, payload);
	});
}

// Sets up the plugin object's mutable state and cross-instance sync via pubsub.
// Must run before any other lib/* attach step since everything else reads/writes these fields.
module.exports = function initState(plugin) {
	plugin.id = 'meilisearch';
	plugin.healthy = false;

	plugin.indexing = {
		running: false,
		topic_progress: { total: null, current: null },
		post_progress: { total: null, current: null },
		message_progress: { total: null, current: null },
		// Meilisearch-side task-queue drain (Phase 2) — populated by lib/drain.js while a
		// drain monitor is attached; rendered as a fourth progress bar in the ACP.
		meili_progress: { total: null, current: null, drained: null },
	};

	plugin.reindexingForced = false;
	plugin.reindexLockRefresh = null;
	// Owner token of the reindex lock while this instance holds it (lib/lock.js).
	// Owner checks on refresh/release keep a stalled-then-recovered holder from
	// extending or deleting a NEW holder's lock.
	plugin.reindexLockToken = null;
	// Whole-run keepalive interval while a reindex runs on THIS instance
	// (lib/reindex.js); keeps the pubsub-synced running state fresh through silent
	// stretches so no instance falsely self-heals mid-run.
	plugin.reindexKeepalive = null;
	// lib/send-documents.js per-run adaptive state; lazily created on first write.
	plugin.meiliSend = null;
	// lib/drain.js drain monitor handle while one is attached.
	plugin.drainMonitor = null;
	// lib/client.js updateEmbedders serialization: the promise of an in-flight
	// embedder push, if any. A host-change save fires a background reindex whose
	// updateEmbedders used to race the save path's own push (both read the just-reset
	// fingerprint and both pushed — the reindex's stale read even overwrote the admin's
	// new config via meilisearch's FIFO). See plugin.updateEmbedders in lib/client.js.
	plugin._embedderPushInFlight = null;
	// True while the one-shot auto-reindex retry (lib/client.js prepareSearch) is
	// scheduled — repeated prepareSearch calls don't stack timers.
	plugin.autoReindexRetryScheduled = false;
	// Last time a 'meilisearch:reindex' pubsub payload arrived (any instance's
	// progress publish). lib/pending-queue.js uses it to detect a reindexing
	// instance that died mid-run without publishing completion — running=true with
	// no updates for > INDEXING_STALE_MS self-heals instead of deferring forever.
	plugin.indexingUpdatedAt = 0;

	/** @type {import('meilisearch').Meilisearch} */
	plugin.client = undefined;
	plugin.defaults = defaults;
	plugin.breakingSettings = breakingSettings;
	plugin.initialized = false;
	plugin.initializingOnAnotherInstance = false;
	plugin.healthCheckTask = null;

	pubsub.on('meilisearch:reindex', (indexing) => {
		plugin.indexing = indexing;
		plugin.indexingUpdatedAt = Date.now();
		emitToAdmins('plugins.meilisearch.reindex', plugin.indexing)
			.catch(err => winston.error(`[plugin/meilisearch] emitToAdmins reindex failed: ${err.message}`));
	});
	pubsub.on('meilisearch:init', (initializing) => {
		plugin.initializingOnAnotherInstance = initializing;
	});
	pubsub.on('meilisearch:alert', (alert) => {
		emitToAdmins('plugins.meilisearch.alert', alert)
			.catch(err => winston.error(`[plugin/meilisearch] emitToAdmins alert failed: ${err.message}`));
	});
};
