'use strict';

const winston = nodebb.require('winston');
const db = nodebb.require('./src/database');
const { PENDING_KEY, PENDING_MAX, DRAIN_MAX, REPLAY_OPS } = require('./constants');

// Persists a write that couldn't be applied to MeiliSearch yet, so it can be replayed later.
async function enqueuePending(op, payload) {
	const count = await db.sortedSetCard(PENDING_KEY);
	if (count >= PENDING_MAX) {
		const oldest = await db.getSortedSetRange(PENDING_KEY, 0, 0);
		if (oldest.length) await db.sortedSetRemove(PENDING_KEY, ...oldest);
	}
	await db.sortedSetAdd(PENDING_KEY, Date.now(), JSON.stringify({ op, payload }));
}

// Decides whether a write hook should be deferred instead of applied immediately:
// buffered in memory during a forced reindex, or queued to the DB while MeiliSearch is unhealthy.
async function maybeDefer(plugin, op, payload) {
	if (plugin.reindexingForced) {
		if (plugin.pendingDuringReindex.length >= PENDING_MAX) {
			plugin.pendingDuringReindex.shift();
		}
		plugin.pendingDuringReindex.push({ op, payload });
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

// Replays queued writes once MeiliSearch is healthy again.
async function drainPending(plugin) {
	if (!plugin.healthy || plugin.reindexingForced) return;
	let drained = 0;
	for (;;) {
		if (!plugin.healthy) break;
		const members = await db.getSortedSetRange(PENDING_KEY, 0, 49);
		if (!members.length) break;
		await db.sortedSetRemove(PENDING_KEY, ...members);
		for (const member of members) {
			try {
				const { op, payload } = JSON.parse(member);
				if (op && REPLAY_OPS.includes(op)) {
					await plugin[op](payload);
				}
			} catch (err) {
				winston.error(`[plugin/meilisearch] replay failed: ${err.message}`);
			}
			drained += 1;
		}
		if (drained >= DRAIN_MAX) break;
	}
}

module.exports = { enqueuePending, maybeDefer, meiliWrite, drainPending };
