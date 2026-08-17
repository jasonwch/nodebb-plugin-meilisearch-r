'use strict';

const Sockets = nodebb.require('./src/socket.io');
const pubsub = nodebb.require('./src/pubsub');
const { defaults, breakingSettings } = require('./defaults');

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
		Sockets.server.to('admin/plugins/meilisearch').emit('plugins.meilisearch.reindex', plugin.indexing);
	});
	pubsub.on('meilisearch:init', (initializing) => {
		plugin.initializingOnAnotherInstance = initializing;
	});
	pubsub.on('meilisearch:alert', (alert) => {
		Sockets.server.to('admin/plugins/meilisearch').emit('plugins.meilisearch.alert', alert);
	});
};
