'use strict';

const winston = nodebb.require('winston');
const settings = nodebb.require('./src/meta/settings');
const Topics = nodebb.require('./src/topics');
const { drainPending } = require('./pending-queue');

async function ensureIndex(plugin, uid, primaryKey) {
	try {
		await plugin.client.getIndex(uid);
	} catch (e) {
		await plugin.client.createIndex(uid, { primaryKey });
	}
}

// MeiliSearch connection lifecycle: connecting, health checks, and index/settings sync.
module.exports = function attachClient(plugin) {
	plugin.patchTopicsSearch = function () {
		if (Topics.__meiliOriginalSearch) { return; }
		Topics.__meiliOriginalSearch = Topics.search;
		Topics.search = async function (tid, term) {
			if (!tid || !term || !String(term).trim()) { return []; }
			return Topics.__meiliOriginalSearch.call(Topics, tid, term);
		};
	};

	plugin.checkHealth = async function () {
		try {
			plugin.healthy = await plugin.client.isHealthy();
			if (!plugin.healthy) {
				winston.warn('[plugin/meilisearch] MeiliSearch host is unhealthy');
			} else {
				drainPending(plugin).catch(err => winston.error(`[plugin/meilisearch] drain failed: ${err.message}`));
			}
			return plugin.healthy;
		} catch (err) {
			plugin.healthy = false;
			winston.warn(`[plugin/meilisearch] ${err.message}`);
			return false;
		}
	};

	// Ensure the chat_message index + filterable attributes exist, independent of reindex state.
	// prepareSearch() returns early when `indexed=true`, so updateIndexSettings (which configures
	// chat_message) is skipped on normal restarts — but indexMessage auto-creates the index via
	// document add, leaving filterableAttributes empty. Without roomId/uid filterable, searchMessages
	// would error on its filter clause. Idempotent + cheap (2 calls, settings deduped by Meilisearch).
	plugin.ensureChatMessageIndex = async function () {
		if (!plugin.client) return;
		try {
			await ensureIndex(plugin, 'chat_message', 'mid');
			await plugin.client.index('chat_message').updateFilterableAttributes(['roomId', 'uid', 'timestamp']);
		} catch (err) {
			winston.error(`[plugin/meilisearch] ensureChatMessageIndex failed: ${err.message}`);
			plugin.healthy = false;
		}
	};

	plugin.prepareSearch = async function (data, connectionChanged = false) {
		winston.debug(`[plugin/meilisearch] Connecting to MeiliSearch host: ${data?.host || await settings.getOne(plugin.id, 'host')}`);
		const { Meilisearch } = await import('meilisearch');
		plugin.client = new Meilisearch({
			host: data?.host || await settings.getOne(plugin.id, 'host'),
			apiKey: data?.apiKey || await settings.getOne(plugin.id, 'apiKey') || undefined,
		});
		if (plugin.healthCheckTask) clearInterval(plugin.healthCheckTask);
		const intervalRaw = parseInt(data?.healthCheckInterval || await settings.getOne(plugin.id, 'healthCheckInterval'), 10);
		const intervalSeconds = Number.isFinite(intervalRaw) && intervalRaw > 0
			? Math.min(Math.max(intervalRaw, 10), 3600)
			: 60;
		plugin.healthCheckTask = setInterval(plugin.checkHealth, intervalSeconds * 1000);
		// #15: Always sync plugin.healthy on (re)connect — otherwise it stays false from
		// initialization until the first setInterval tick, causing stale "MeiliSearch
		// Unreachable" warnings on save during the startup window (MS online, flag stale).
		await plugin.checkHealth();
		// #14: Don't auto-reindex after a failed reindex unless connection settings changed.
		const indexed = await settings.getOne(plugin.id, 'indexed');
		if (indexed) return;
		if (!plugin.healthy || plugin.initializingOnAnotherInstance) return;
		const lastResult = await settings.getOne(plugin.id, 'lastReindexResult') || {};
		const allowAutoReindex = connectionChanged || lastResult.success !== false;
		if (!allowAutoReindex) {
			winston.warn('[plugin/meilisearch] Skipping auto-reindex: last reindex failed. Trigger manually from ACP.');
			return;
		}
		await plugin.updateIndexSettings();
		await plugin.reindex(false);
	};

	plugin.updateIndexSettings = async (data) => {
		await ensureIndex(plugin, 'post', 'pid');
		await ensureIndex(plugin, 'topic', 'tid');
		await ensureIndex(plugin, 'chat_message', 'mid');
		data = {
			maxDocuments: parseInt(data?.maxDocuments || await settings.getOne(plugin.id, 'maxDocuments') || 500, 10),
			rankingRules: (data?.rankingRules || await settings.getOne(plugin.id, 'rankingRules'))?.map(value => value.rule),
			stopWords: (data?.stopWords || await settings.getOne(plugin.id, 'stopWords'))?.map(value => value.word),
			typoTolerance: ['on', true].includes(
				data?.typoTolerance || await settings.getOne(plugin.id, 'typoTolerance') || undefined,
			),
			typoToleranceMinWordSizeOneTypo: parseInt(
				data?.typoToleranceMinWordSizeOneTypo ||
					await settings.getOne(plugin.id, 'typoToleranceMinWordSizeOneTypo') || 5,
				10,
			),
			typoToleranceMinWordSizeTwoTypos: parseInt(
				data?.typoToleranceMinWordSizeTwoTypos ||
					await settings.getOne(plugin.id, 'typoToleranceMinWordSizeTwoTypos') || 9,
				10,
			),
			typoToleranceDisableOnWords:
				(data?.typoToleranceDisableOnWords || await settings.getOne(plugin.id, 'typoToleranceDisableOnWords') ||
					undefined)?.map(value => value.word),
			synonyms: Object.fromEntries(
				(data?.synonyms || await settings.getOne(plugin.id, 'synonyms') || [])?.map((
					{ word, synonyms },
				) => [word, synonyms?.split(',').map(synonym => synonym.trim())]),
			),
		};
		const postTask = await plugin.client.index('post').updateSettings({
			filterableAttributes: ['tid', 'cid', 'uid', 'timestamp'],
			sortableAttributes: ['timestamp', 'cid'],
			searchableAttributes: ['content'],
			pagination: {
				maxTotalHits: data.maxDocuments,
			},
			rankingRules: data.rankingRules,
			stopWords: data.stopWords,
			typoTolerance: {
				enabled: data.typoTolerance,
				minWordSizeForTypos: {
					oneTypo: data.typoToleranceMinWordSizeOneTypo,
					twoTypos: data.typoToleranceMinWordSizeTwoTypos,
				},
				disableOnWords: data.typoToleranceDisableOnWords,
			},
			synonyms: data.synonyms,
		});
		const topicTask = await plugin.client.index('topic').updateSettings({
			filterableAttributes: ['cid', 'uid', 'timestamp'],
			sortableAttributes: ['cid', 'title', 'timestamp'],
			searchableAttributes: ['title'],
			pagination: {
				maxTotalHits: data.maxDocuments,
			},
			rankingRules: data.rankingRules,
			stopWords: data.stopWords,
			typoTolerance: {
				enabled: data.typoTolerance,
				minWordSizeForTypos: {
					oneTypo: data.typoToleranceMinWordSizeOneTypo,
					twoTypos: data.typoToleranceMinWordSizeTwoTypos,
				},
				disableOnWords: data.typoToleranceDisableOnWords,
			},
			synonyms: data.synonyms,
		});
		const messageTask = await plugin.client.index('chat_message').updateSettings({
			filterableAttributes: ['roomId', 'uid', 'timestamp'],
			sortableAttributes: ['timestamp'],
			searchableAttributes: ['content'],
			pagination: {
				maxTotalHits: data.maxDocuments,
			},
			rankingRules: data.rankingRules,
			stopWords: data.stopWords,
			typoTolerance: {
				enabled: data.typoTolerance,
				minWordSizeForTypos: {
					oneTypo: data.typoToleranceMinWordSizeOneTypo,
					twoTypos: data.typoToleranceMinWordSizeTwoTypos,
				},
				disableOnWords: data.typoToleranceDisableOnWords,
			},
			synonyms: data.synonyms,
		});
		return [postTask, topicTask, messageTask];
	};
};
