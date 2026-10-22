'use strict';

// Nested-object settings must survive NodeBB's Redis adapter, which stringifies
// every hash value: String({...}) => '[object Object]' (unparseable, and a strict-mode
// property write on that string throws), String(false) => 'false'. Mongo (BSON) and
// Postgres (JSONB) round-trip objects natively (F8). Normalizing to JSON strings on
// write + tolerant parsing on read gives identical, working behavior on every adapter.
//
// Migration tolerance: a Redis deployment upgraded from a pre-fix version has the
// unrecoverable '[object Object]' string stored — reads fall back to the provided
// default instead of throwing, and the next write re-persists usable JSON.

const settings = nodebb.require('./src/meta/settings');

async function getJsonSetting(plugin, key, fallback) {
	const raw = await settings.getOne(plugin.id, key);
	if (raw === null || raw === undefined) {
		return fallback;
	}
	if (typeof raw === 'object') {
		return raw; // mongo/postgres native object
	}
	if (typeof raw === 'string') {
		try {
			const parsed = JSON.parse(raw);
			return (parsed && typeof parsed === 'object') ? parsed : fallback;
		} catch (err) {
			// legacy Redis '[object Object]' (or other unparsable legacy value)
			return fallback;
		}
	}
	return fallback;
}

async function setJsonSetting(plugin, key, value) {
	await settings.set(plugin.id, { [key]: JSON.stringify(value) }, true);
}

module.exports = { getJsonSetting, setJsonSetting };
