'use strict';

// Cross-instance reindex lock with owner tokens (flaw 3 fix).
//
// The pre-fix lock was a plain string (acquire-time Date.now()) with unconditional
// refresh/release. That had two corruption paths once a holder stalled past the TTL:
//   - the stale holder's periodic db.expire extended the NEW holder's lock, and
//   - the stale holder's finally db.delete removed the new holder's lock mid-run
//     (letting a third run start inside the second).
// The lock value is now JSON { owner, acquiredAt }; refresh and release verify the
// owner before touching the key, so a recovered stale holder can only log and stop.
// Legacy plain-timestamp values (from an in-flight pre-upgrade run) are parsed
// leniently for the held-age admin alert.
//
// Known residual, accepted deliberately: acquire is still check-then-set
// (db.ttl -> db.set -> db.expire) because NodeBB's db abstraction exposes no
// portable atomic SET NX. Two acquirers in the same instant can both "win". With
// single-admin triggering (boot is isPrimary-gated, the one-shot retry is
// primary-gated and consumed per process) that window is practically unreachable,
// and its consequence — two concurrent reindex runs — wastes work but corrupts
// nothing (batches are idempotent upserts/deletes, replay is FIFO last-wins).

const crypto = require('node:crypto');
const winston = nodebb.require('winston');
const db = nodebb.require('./src/database');
const { REINDEX_LOCK_KEY, REINDEX_LOCK_TTL, LOCK_REFRESH_INTERVAL, REINDEX_HEARTBEAT_KEY, REINDEX_HEARTBEAT_STALE_MS } = require('./constants');

// Lock value formats:
//   new:    JSON.stringify({ owner: <uuid>, acquiredAt: <Date.now()> })
//   legacy: String(<acquire-time Date.now()>)
function parseLockValue(value) {
	if (!value) return null;
	try {
		const parsed = JSON.parse(value);
		if (parsed && typeof parsed.owner === 'string' && Number.isFinite(parsed.acquiredAt)) {
			return parsed;
		}
	} catch (err) {
		// fall through to legacy parse
	}
	const acquiredAt = parseInt(value, 10);
	return Number.isFinite(acquiredAt) ? { owner: null, acquiredAt } : null;
}

// Reindex liveness heartbeat: the run's keepalive writes a timestamp every
// REINDEX_KEEPALIVE_MS (plus once at acquisition); releaseReindexLock deletes it.
// Readers: acquireReindexLock (dead-holder reclaim) and maybeDefer (false
// self-heal gate) — the DB is the ground truth when pubsub delivery fails.
async function writeReindexHeartbeat() {
	try {
		await db.set(REINDEX_HEARTBEAT_KEY, String(Date.now()));
	} catch (err) {
		// non-fatal: staleness detection falls back to the conservative path
	}
}

async function isReindexHeartbeatFresh() {
	try {
		const ts = parseInt(await db.get(REINDEX_HEARTBEAT_KEY), 10);
		return Number.isFinite(ts) && (Date.now() - ts) < REINDEX_HEARTBEAT_STALE_MS;
	} catch (err) {
		return false;
	}
}

// True when the reindex lock still carries THIS run's owner token — used to gate
// the run's finally block (a recovered run whose lock a successor took over must
// not drain the shared queue or publish a phantom completion mid-successor-run).
async function isReindexLockOwnedBy(plugin) {
	try {
		const parsed = parseLockValue(await db.get(REINDEX_LOCK_KEY));
		return !!(parsed && plugin.reindexLockToken && parsed.owner === plugin.reindexLockToken);
	} catch (err) {
		return false;
	}
}

async function acquireReindexLock(plugin) {
	const ttl = await db.ttl(REINDEX_LOCK_KEY);
	if (ttl > 0) {
		// Dead-holder reclaim: the 5-min refresh RESETS the lock TTL, so a crashed
		// holder leaves up to REINDEX_LOCK_TTL of blockage behind. A STALE heartbeat
		// (present but older than REINDEX_HEARTBEAT_STALE_MS) proves the holder is
		// dead — steal the lock; the owner-token checks make the dead holder's
		// later refresh/release no-ops. A MISSING heartbeat is treated as a live
		// pre-heartbeat-version holder (conservative — refuse).
		let holderIsDead = false;
		try {
			const heartbeatTs = parseInt(await db.get(REINDEX_HEARTBEAT_KEY), 10);
			holderIsDead = Number.isFinite(heartbeatTs) && (Date.now() - heartbeatTs) >= REINDEX_HEARTBEAT_STALE_MS;
		} catch (err) {
			holderIsDead = false;
		}
		if (holderIsDead) {
			winston.warn('[plugin/meilisearch] Reindex lock held by a DEAD holder (heartbeat stale) — reclaiming it');
			try {
				await db.delete(REINDEX_LOCK_KEY);
				await db.delete(REINDEX_HEARTBEAT_KEY);
			} catch (err) {
				winston.warn(`[plugin/meilisearch] Could not reclaim the dead holder's lock: ${err.message}`);
				holderIsDead = false;
			}
		}
		if (!holderIsDead) {
			winston.warn('[plugin/meilisearch] Reindex lock held by another instance');
		// Surface it to admins too — the click silently did nothing. Include the hold
		// age (from the lock value's acquire time) so an admin can tell "another
		// instance is running" from "this lock looks stale". Throttled to one toast
		// per minute by notifyAdmins.
		try {
			const lockVal = await db.get(REINDEX_LOCK_KEY);
			const parsed = parseLockValue(lockVal);
			const heldMs = parsed ? Date.now() - parsed.acquiredAt : null;
			const heldMin = Number.isFinite(heldMs) && heldMs >= 0 ? Math.max(1, Math.round(heldMs / 60000)) : null;
			plugin.notifyAdmins('reindex:lockRefused', {
				type: 'warning',
				titleKey: '[[meilisearch:admin.reindexLockHeld]]',
				messageKey: heldMin ? 'meilisearch:admin.reindexLockHeldBody' : 'meilisearch:admin.reindexLockHeldBodyNoAge',
				messageArgs: heldMin ? [heldMin] : [],
				message: heldMin ?
					'Another instance holds the reindex lock (held for ~' + heldMin + ' min; it auto-expires within 60 min if that instance died). If no reindex is actually running, wait for expiry or clear the lock key.' :
					'Another instance holds the reindex lock.',
			});
		} catch (err) {
			winston.warn(`[plugin/meilisearch] Could not alert admins about the held lock: ${err.message}`);
		}
			return false;
		}
	}
	const token = crypto.randomUUID();
	await db.set(REINDEX_LOCK_KEY, JSON.stringify({ owner: token, acquiredAt: Date.now() }));
	await db.expire(REINDEX_LOCK_KEY, REINDEX_LOCK_TTL);
	plugin.reindexLockToken = token;
	plugin.reindexLockRefresh = setInterval(() => {
		refreshReindexLock(plugin).catch(err =>
			winston.error(`[plugin/meilisearch] lock refresh failed: ${err.message}`));
	}, LOCK_REFRESH_INTERVAL);
	return true;
}

// Owner-checked TTL refresh: a holder that stalled past its own lock's expiry (the
// lock was re-acquired elsewhere in the meantime) must not extend the new holder's
// TTL — instead it stops refreshing; its run's release will likewise no-op.
async function refreshReindexLock(plugin) {
	try {
		const value = await db.get(REINDEX_LOCK_KEY);
		const parsed = parseLockValue(value);
		if (!parsed || parsed.owner !== plugin.reindexLockToken) {
			winston.error('[plugin/meilisearch] Reindex lock no longer owned by this run — stopping refresh');
			if (plugin.reindexLockRefresh) {
				clearInterval(plugin.reindexLockRefresh);
				plugin.reindexLockRefresh = null;
			}
			return;
		}
		await db.expire(REINDEX_LOCK_KEY, REINDEX_LOCK_TTL);
	} catch (err) {
		winston.error(`[plugin/meilisearch] lock refresh failed: ${err.message}`);
	}
}

// Owner-checked release: a stale holder completing after a new run acquired the
// (expired) lock must not delete the new holder's lock mid-run.
async function releaseReindexLock(plugin) {
	if (plugin.reindexLockRefresh) {
		clearInterval(plugin.reindexLockRefresh);
		plugin.reindexLockRefresh = null;
	}
	const token = plugin.reindexLockToken;
	plugin.reindexLockToken = null;
	try {
		const value = await db.get(REINDEX_LOCK_KEY);
		const parsed = parseLockValue(value);
		if (parsed && token && parsed.owner !== null && parsed.owner !== token) {
			// A successor holds the lock (and its own heartbeat) — touch neither.
			winston.warn('[plugin/meilisearch] Not releasing the reindex lock — it is held by another run');
			return;
		}
		await db.delete(REINDEX_LOCK_KEY);
		// Our run is over — clear its heartbeat too (a successor writes its own at
		// acquisition).
		await db.delete(REINDEX_HEARTBEAT_KEY);
	} catch (err) {
		winston.warn(`[plugin/meilisearch] Could not release reindex lock: ${err.message}`);
	}
}

module.exports = { acquireReindexLock, refreshReindexLock, releaseReindexLock, parseLockValue, writeReindexHeartbeat, isReindexHeartbeatFresh, isReindexLockOwnedBy };
