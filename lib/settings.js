'use strict';

const winston = nodebb.require('winston');
const settings = nodebb.require('./src/meta/settings');
const { clampMinTermLength } = require('./settings-helpers');
const { SEMANTIC_SETTING_KEYS, resetAppliedConfigs } = require('./embedder');
const { redactSettingForLog, redactSecrets } = require('./redact');

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
			// M5 fix: redact credential-bearing fields (apiKey, REST templates) from winston diff log
			// so admin key rotation doesn't leak old+new paid OpenAI keys to output.log/aggregators
			// via the ACP Logs page and any streaming log aggregator (Loki/ELK/Datadog).
			winston.info(`[plugin/meilisearch] ${setting} changed: ${redactSettingForLog(setting, stored)} -> ${redactSettingForLog(setting, value)}`);
		}
		return changed;
	}

	// Deliberately separate from isBreaking()/updateIndexSettings: embedder config
	// changes are the only thing that can make Meilisearch re-embed every document
	// through a (often paid) embedding API, so this must only fire when a semantic
	// search field was itself part of the diff - never as a side effect of saving
	// unrelated settings like ranking rules or stop words.
	async function semanticSettingsChanged(newSettings) {
		if (!newSettings) return false;
		const results = await Promise.all(SEMANTIC_SETTING_KEYS.map(async (key) => {
			if (!Object.prototype.hasOwnProperty.call(newSettings, key)) return false;
			const stored = await settings.getOne(plugin.id, key);
			return !deepCompare(stored, newSettings[key]);
		}));
		return results.some(Boolean);
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
						// The new host has no memory of what embedder config this plugin last
						// applied to the OLD host - without this, an unchanged semantic search
						// setting would look "already applied" and never get pushed to the new host.
						await resetAppliedConfigs(plugin);
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
				if (await semanticSettingsChanged(data.settings)) {
					winston.info('[plugin/meilisearch] semantic search settings changed, updating embedders');
					await plugin.updateEmbedders(data.settings);
				}
			} catch (err) {
				// #7: Don't throw — let settings persist to DB even when Meili is unreachable.
				// C1 fix: redact any apiKey that Meilisearch may echo in its error message before
				// logging. Falls back to pattern-only redaction if settings DB is unreachable.
				// A3-style inner try/catch: fall back to raw err.message if redaction itself fails,
				// so the error still surfaces (just unredacted) rather than silently swallowing it.
				const rawReason = err.message;
				let safeReason;
				try {
					safeReason = await redactSecrets(rawReason, plugin);
				} catch {
					safeReason = rawReason;
				}
				winston.error(`[plugin/meilisearch] Error while saving settings: ${safeReason}`);
				plugin.healthy = false;
				// Surface the failure to admins in the ACP — without this, the client sees the
				// "Settings Saved" success toast even when Meilisearch silently rejected the
				// settings/embedder update. Client-side validation (admin.js) is the primary
				// UX path for invalid configs; this catches connection-level failures (e.g.
				// Meilisearch host unreachable after Fix B's 30s HTTP timeout fires).
				// C1 fix: surface the failure to admins without leaking credentials. The error
				// message may contain the apiKey if Meilisearch echoes it in 401 responses.
				plugin.notifyAdmins('settings:save', {
					type: 'danger',
					titleKey: '[[meilisearch:admin.meilisearchError]]',
					message: safeReason,
				});
			}
		}
		return data;
	};
};
