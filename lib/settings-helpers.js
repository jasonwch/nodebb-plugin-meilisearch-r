'use strict';

const settings = nodebb.require('./src/meta/settings');

async function resolveMinTermLength(plugin) {
	const v = parseInt(await settings.getOne(plugin.id, 'searchMinTermLength'), 10);
	return Number.isFinite(v) && v >= 2 ? v : 2;
}

function clampMinTermLength(value) {
	const v = parseInt(value, 10);
	return Number.isFinite(v) && v >= 2 ? v : 2;
}

// NodeBB's Redis adapter stringifies every hash value (String(false) => 'false'),
// so a stored `false` reads back as the TRUTHY string 'false' — a bare truthiness
// check on the `indexed` flag then wrongly reports "already indexed" and silently
// skips the boot/host-change auto-reindex (F7). Mongo (BSON) and Postgres (JSONB)
// round-trip native booleans, where this helper accepts them unchanged.
function isSettingTrue(value) {
	return value === true || value === 'true' || value === 1 || value === '1';
}

module.exports = { resolveMinTermLength, clampMinTermLength, isSettingTrue };
