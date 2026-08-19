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
	};

	plugin.reindexingForced = false;
	plugin.pendingDuringReindex = [];
	plugin.reindexLockRefresh = null;

	/** @type {import('meilisearch').Meilisearch} */
	plugin.client = undefined;
	plugin.defaults = defaults;
	plugin.breakingSettings = breakingSettings;
	plugin.initialized = false;
	plugin.initializingOnAnotherInstance = false;
	plugin.healthCheckTask = null;

	pubsub.on('meilisearch:reindex', (indexing) => {
		plugin.indexing = indexing;
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
