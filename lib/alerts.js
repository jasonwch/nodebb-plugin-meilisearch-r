'use strict';

const pubsub = nodebb.require('./src/pubsub');

// Throttle window per alert key, so a hot failure path (e.g. every search hitting a
// broken embedder) doesn't flood every open ACP tab with a repeat toast on every request.
const THROTTLE_MS = 60 * 1000;
const lastSentAt = new Map();

// Real-time admin notifications, separate from winston logging: pushed over pubsub so
// every cluster instance's connected ACP sockets show the toast (lib/state.js relays
// 'meilisearch:alert' to the 'admin/plugins/meilisearch' room), not just the instance
// that hit the error.
module.exports = function attachAlerts(plugin) {
	// `key` identifies the failure (e.g. "embedder:post") so repeats of the SAME issue are
	// throttled independently from other issues, which should still surface immediately.
	plugin.notifyAdmins = function notifyAdmins(key, alert) {
		const now = Date.now();
		const last = lastSentAt.get(key) || 0;
		if (now - last < THROTTLE_MS) return;
		lastSentAt.set(key, now);
		pubsub.publish('meilisearch:alert', alert);
	};
};
