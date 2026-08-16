'use strict';

const winston = nodebb.require('winston');
const settings = nodebb.require('./src/meta/settings');
const { clampMinTermLength } = require('./settings-helpers');

function deepCompare(a, b) {
	if (a === null || b === null) return a === b;
	if (typeof a !== typeof b) return false;
	switch (typeof a) {
		case 'object':
			return Object.keys(a).length === Object.keys(b).length &&
				Object.keys(a).every(key => deepCompare(a[key], b[key]));
		case 'string':
		case 'number':
		case 'boolean':
		default:
			return a === b;
	}
}

// filter:settings.set — reacts to ACP settings saves: reconnects on connection-setting
// changes and pushes breaking settings (ranking rules, typo tolerance, etc.) to MeiliSearch.
module.exports = function attachSettings(plugin) {
	async function connectionSettingsChanged(newSettings) {
		if (!newSettings) return false;
		const keys = ['host', 'apiKey', 'healthCheckInterval'];
		for (const k of keys) {
			if (!Object.prototype.hasOwnProperty.call(newSettings, k)) continue;
			const stored = await settings.getOne(plugin.id, k);
			if (String(stored || '') !== String(newSettings[k] || '')) return true;
		}
		return false;
	}

	async function hostSettingChanged(newSettings) {
		if (!newSettings || !Object.prototype.hasOwnProperty.call(newSettings, 'host')) return false;
		const stored = await settings.getOne(plugin.id, 'host');
		return String(stored || '') !== String(newSettings.host || '');
	}

	async function isBreaking([setting, value]) {
		if (!plugin.breakingSettings.includes(setting)) {
			return false;
		}
		const stored = await settings.getOne(plugin.id, setting);
		const changed = !deepCompare(stored, value);
		if (changed) {
			winston.info(`[plugin/meilisearch] ${setting} changed: ${JSON.stringify(stored)} -> ${JSON.stringify(value)}`);
		}
		return changed;
	}

	plugin.saveSettings = async (data) => {
		if (data.plugin === plugin.id && !data.quiet && plugin.initialized) {
			try {
				if (data.settings && Object.prototype.hasOwnProperty.call(data.settings, 'searchMinTermLength')) {
					data.settings.searchMinTermLength = clampMinTermLength(data.settings.searchMinTermLength);
				}
				// #8: Only re-connect when connection settings changed.
				const connChanged = await connectionSettingsChanged(data.settings);
				if (connChanged) {
					// Fix 10: If host changed, clear indexed so prepareSearch auto-reindexes the new (empty) host.
					const hostChanged = await hostSettingChanged(data.settings);
					if (hostChanged) {
						await settings.set(plugin.id, { indexed: false }, true);
					}
					await plugin.prepareSearch(data.settings, true);
				}
				const changed = (await Promise.all(
					Object.entries(data.settings).map(([k, v]) => isBreaking([k, v])),
				)).some(Boolean);
				if (changed) {
					winston.info('settings changed, updating index');
					await plugin.updateIndexSettings(data.settings);
				}
			} catch (err) {
				// #7: Don't throw — let settings persist to DB even when Meili is unreachable.
				winston.error(`[plugin/meilisearch] Error while saving settings: ${err.message}`);
				plugin.healthy = false;
			}
		}
		return data;
	};
};
