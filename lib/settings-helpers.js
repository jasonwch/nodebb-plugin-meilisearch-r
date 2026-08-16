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

module.exports = { resolveMinTermLength, clampMinTermLength };
