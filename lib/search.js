'use strict';

const winston = nodebb.require('winston');
const settings = nodebb.require('./src/meta/settings');
const plugins = nodebb.require('./src/plugins');
const { resolveMinTermLength } = require('./settings-helpers');
const { EMBEDDER_NAME, isSemanticSearchEnabled, getSemanticRatio, getSemanticScoreThreshold } = require('./embedder');
const { redactSecrets } = require('./redact');
const { decodeMid, decodePid } = require('./id-codec');
const { filterValue } = require('./meili-filter');

// rankingScoreThreshold is a top-level search parameter (a sibling of "hybrid", not nested
// inside it) - Meilisearch defaults it to 0 (no filtering) when omitted, which is why
// unfiltered vector search returns its full `limit` regardless of how unrelated the tail is.
async function buildSemanticSearchExtras(plugin) {
	if (!(await isSemanticSearchEnabled(plugin))) return {};
	return {
		hybrid: { embedder: EMBEDDER_NAME, semanticRatio: await getSemanticRatio(plugin) },
		rankingScoreThreshold: await getSemanticScoreThreshold(plugin),
	};
}

// Runs a Meilisearch query and never lets it throw past this point: a broken/misconfigured
// embedder (e.g. "Cannot find embedder with name `default`") must not take down normal
// keyword search or callers like Topics.getSuggestedTopics, which propagate rejections
// straight up through NodeBB's hook chain. If hybrid search fails, retries once as a plain
// keyword search so users still get results instead of nothing; if that also fails (e.g.
// Meilisearch itself is down), logs + alerts once (throttled) and returns null so the
// caller can fall back to its original, unmodified data.
async function safeSearch(plugin, indexName, term, opts) {
	try {
		return await plugin.client.index(indexName).search(term, opts);
	} catch (err) {
		const reason = err?.message || String(err);
		// B1 fix: redact any apiKey Meilisearch may echo in search error responses.
		// Search errors typically return code+message rather than echoing credentials,
		// but uniform posture with the embedder-task paths is cheap defense-in-depth.
		// A3-style inner try/catch preserves safeSearch's "never throw" contract: if
		// redaction itself fails, fall back to the raw reason rather than rejecting.
		let safeReason;
		try {
			safeReason = await redactSecrets(reason, plugin);
		} catch {
			safeReason = reason;
		}
		if (opts.hybrid) {
			winston.error(`[plugin/meilisearch] Hybrid search on "${indexName}" failed (${safeReason}); falling back to keyword-only search`);
			plugin.notifyAdmins(`search:${indexName}`, {
				type: 'danger',
				titleKey: '[[meilisearch:admin.semanticSearchDegraded]]',
				message: `${indexName}: ${safeReason}`,
			});
			// rankingScoreThreshold is tuned for hybrid/vector scores - drop it too, or a
			// keyword-only fallback could wrongly filter out valid BM25-style matches.
			const { hybrid, rankingScoreThreshold, ...keywordOnly } = opts;
			try {
				return await plugin.client.index(indexName).search(term, keywordOnly);
			} catch (fallbackErr) {
				const fallbackReason = fallbackErr?.message || String(fallbackErr);
				let safeFallbackReason;
				try {
					safeFallbackReason = await redactSecrets(fallbackReason, plugin);
				} catch {
					safeFallbackReason = fallbackReason;
				}
				winston.error(`[plugin/meilisearch] Keyword-only fallback search on "${indexName}" also failed: ${safeFallbackReason}`);
				return null;
			}
		}
		winston.error(`[plugin/meilisearch] Search on "${indexName}" failed: ${safeReason}`);
		plugin.notifyAdmins(`search:${indexName}`, {
			type: 'danger',
			titleKey: '[[meilisearch:admin.semanticSearchDegraded]]',
			message: `${indexName}: ${safeReason}`,
		});
		return null;
	}
}

// Core search integration: filter:search.query / filter:topic.search / filter:messaging.searchMessages.
module.exports = function attachSearch(plugin) {
	plugin.checkConflict = function () {
		const hooksToCheck = [
			'filter:search.query',
			'filter:topic.search',
			'filter:messaging.searchMessages',
		];
		// blacklist, in case someone makes a plugin using these hooks that doesn't conflict.
		// also, outside of dbsearch the user is expected to realize they installed two search plugins.
		const conflictingPlugins = [
			'nodebb-plugin-dbsearch',
			'nodebb-plugin-solr',
			'nodebb-plugin-elasticsearch',
			'nodebb-plugin-search-elasticsearch',
		];
		for (const hook of hooksToCheck) {
			if ((plugins.loadedHooks[hook] || []).filter(hookData => conflictingPlugins.includes(hookData.id)).length >= 1) {
				return true;
			}
		}
		return false;
	};

	plugin.searchMessages = async function (data) {
		if (!data || !data.content) {
			return data;
		}
		if (plugin.checkConflict()) {
			winston.warn('[plugin/meilisearch] Another search plugin (most likely dbsearch) is enabled, so chat search via Meilisearch was aborted.');
			return data;
		}
		if (!plugin.healthy && !(await plugin.checkHealth())) {
			winston.warn('[plugin/meilisearch] Meilisearch instance did not return a healthy response, so chat search via Meilisearch was aborted.');
			return data;
		}
		const rawQuery = String(data.content).trim();
		const minTermLength = await resolveMinTermLength(plugin);
		if (!rawQuery || !rawQuery.split(' ').some(word => word.length >= minTermLength)) {
			winston.debug(`[plugin/meilisearch] Skipping chat search: no query word >= ${minTermLength} char(s)`);
			return data;
		}
		const num = (v) => {
			const n = parseInt(v, 10);
			return Number.isFinite(n) ? n : null;
		};
		const filter = [];
		// Core passes roomId/uid as arrays (filter:messaging.searchMessages payload in src/api/search.js).
		const roomIds = Array.isArray(data.roomId) ? data.roomId : [data.roomId];
		const roomNums = roomIds.map(num).filter(r => r !== null);
		// Fail closed: if no valid roomId, return no results rather than searching globally across all rooms.
		if (!roomNums.length) {
			winston.warn('[plugin/meilisearch] searchMessages called with no valid roomId; returning no results');
			return data;
		}
		filter.push(roomNums.map(rid => `roomId = ${rid}`));
		// uid is an optional sender filter (message author, not the searcher); never
		// populated by core callers. Remote authors carry URL-shaped uids — quoted
		// string literals, not dropped (lib/meili-filter.js).
		const uids = Array.isArray(data.uid) ? data.uid : [data.uid];
		const uidValues = uids
			.map(v => (v == null ? null : String(v)))
			.filter(v => v && v !== '');
		if (uidValues.length) {
			filter.push(uidValues.map(u => `uid = ${filterValue(u)}`));
		}
		const limit = parseInt(await settings.getOne(plugin.id, 'maxDocuments') || 100, 10);
		winston.debug(`[plugin/meilisearch] Searching chat messages for "${rawQuery}" in room ${roomNums.join(',')}`);
		const result = await safeSearch(plugin, 'chat_message', rawQuery, {
			attributesToRetrieve: ['mid'],
			limit: Math.min(limit, 100),
			filter: filter.length ? filter : undefined,
			matchingStrategy: data.matchWords === 'all' ? 'all' : 'last',
			...(await buildSemanticSearchExtras(plugin)),
		});
		if (!result) return data;
		// Remote (URL) mids are stored under their reversible codec form — decode
		// back to the DB key so core's messaging.getMessagesData(ids) can resolve them.
		data.ids = data.ids.concat(result.hits.map(hit => decodeMid(hit.mid)));
		return data;
	};

	plugin.search = async function (data) {
		if (plugin.checkConflict()) {
			// The dbsearch plugin was detected, abort search!
			winston.warn(
				'[plugin/meilisearch] Another search plugin (most likely dbsearch) is enabled, so search via Meilisearch was aborted.',
			);
			return data;
		}
		if (!plugin.healthy && !(await plugin.checkHealth())) {
			winston.warn(
				'[plugin/meilisearch] Meilisearch instance did not return a healthy response, so search via Meilisearch was aborted.',
			);
			return data;
		}
		const rawQuery = (data.term || data.content || '').trim();
		const minTermLength = await resolveMinTermLength(plugin);
		if (!rawQuery || !rawQuery.split(' ').some(word => word.length >= minTermLength)) {
			winston.debug(`[plugin/meilisearch] Skipping search: no query word >= ${minTermLength} char(s)`);
			return data;
		}
		// #6: Use local variables instead of mutating the shared payload.
		const content = data.term || data.content;
		const searchData = data.term ? { tid: data.tid } : data?.searchData;
		const rawIndex = Array.isArray(data?.index) ? data.index[0] : data.index;
		const ALLOWED_INDEXES = { post: 'pid', topic: 'tid' };
		const index = ALLOWED_INDEXES[rawIndex] ? rawIndex : 'post';
		const id = ALLOWED_INDEXES[index];
		winston.debug(`[plugin/meilisearch] Searching for ${content} in ${index}`);
		const result = await safeSearch(plugin, index, content, {
			attributesToRetrieve: [id],
			limit: parseInt(await settings.getOne(plugin.id, 'maxDocuments') || 500, 10),
			filter: plugin.buildFilter(
				data.cid,
				data.uid,
				searchData?.timeFilter,
				searchData?.timeRange,
				searchData?.tid,
			),
			sort: plugin.buildSort(searchData?.sortBy, searchData?.sortDirection, index),
			matchingStrategy: data.matchWords === 'all' ? 'all' : 'last',
			...(await buildSemanticSearchExtras(plugin)),
		});
		if (!result) return data;
		// Post-index hits carry codec-form pids for remote (URL) posts — decode back
		// to the DB keys core expects. Topic tids (local numeric or remote UUID)
		// are the real DB tids and pass through verbatim.
		data.ids = result.hits.map(hit => (index === 'post' ? decodePid(hit[id]) : hit[id]));
		return data;
	};

	// Meilisearch filter clauses for the post/topic indexes.
	//
	// Federation notes (lib/meili-filter.js filterValue):
	//   - tid: local topics have numeric tids; remote (federated) topics carry
	//     locally-generated UUID tids — quoted string literals. Dropping
	//     non-numeric tids (the pre-federation behavior) turned an in-topic
	//     search (filter:topic.search) into an UNFILTERED query that leaked
	//     matching posts from every other topic.
	//   - uid: remote authors carry URL-shaped actor ids — quoted, not dropped,
	//     so postedBy filtering works for federated users.
	//   - cid: local and world (-1, the ActivityPub pseudo-category for
	//     unclassified remote content) cids are numeric and stay bare; remote
	//     categories carry URL cids (categoryRemote:<url>, selectable in the
	//     search page's category filter) — quoted, not dropped. Dropping the
	//     clause fail-opened a remote-category search into every category's
	//     results.
	plugin.buildFilter = function (categories, postedBy, timeFilter, timeRange, tid) {
		const num = (v) => {
			const n = parseInt(v, 10);
			return Number.isFinite(n) ? n : null;
		};
		const filter = [];
		if (categories?.length) {
			const cids = categories
				.map(v => (v == null ? null : String(v)))
				.filter(v => v && v !== '');
			if (cids.length) filter.push(cids.map(c => `cid = ${filterValue(c)}`));
		}
		if (postedBy?.length) {
			const uids = postedBy
				.map(v => (v == null ? null : String(v)))
				.filter(v => v && v !== '');
			if (uids.length) filter.push(uids.map(u => `uid = ${filterValue(u)}`));
		}
		if (timeFilter && timeRange) {
			const range = num(timeRange);
			if (range !== null) {
				filter.push(`timestamp ${timeFilter === 'newer' ? '>' : '<'} ${Date.now() - (range * 1000)}`);
			}
		}
		if (tid !== undefined && tid !== null && String(tid) !== '') {
			filter.push(`tid = ${filterValue(tid)}`);
		}
		return filter.length ? filter : undefined;
	};

	// Sort parameter for the Meilisearch query. Two facts drive this mapping:
	//
	// 1. Core's sortDirection values are 'asc' | 'desc' (src/controllers/search.js
	//    validParams) — an earlier revision compared against 'ascending', a value
	//    core never sends, so EVERY sort was silently sent as desc. Display order
	//    was still correct (core's filterAndSort/sortPosts re-sorts the returned
	//    pids itself), but with more matches than maxDocuments Meilisearch
	//    truncates to its top-N by the requested sort — an ascending "post time"
	//    search kept the NEWEST window instead of the oldest. Sort only by the
	//    semantic key actually present in the index, never a proxy: sorting by
	//    something else reorders only the truncation window, not the final list.
	// 2. Sortable attributes (lib/client.js updateIndexSettings): post index
	//    ['timestamp', 'cid'], topic index ['cid', 'title', 'timestamp']. 'title'
	//    is sortable ONLY on the topic index — sending a title sort to the post
	//    index is a Meilisearch 400 ("Attribute `title` is not sortable") which
	//    killed ALL post hits for titlesposts searches sorted by topic title
	//    (posts carry no title field at all; the topic index keeps title sorting).
	//
	// Everything without a sortable equivalent in the index ('relevance' — the
	// default, 'votes', 'topic.lastposttime', 'topic.timestamp', 'user.username',
	// 'category.name' — cid order is NOT name order, ...) returns undefined:
	// relevance-ordered truncation, with core re-sorting the final list either way.
	plugin.buildSort = function (sortBy, sortDirection, index) {
		// Core sends 'asc' | 'desc'; treat anything else as desc (defensive).
		const direction = sortDirection === 'asc' ? 'asc' : 'desc';
		switch (sortBy) {
			case 'timestamp':
				// Post timestamp — sortable on both indexes.
				return [`timestamp:${direction}`];
			case 'topic.title':
				// Topic title — sortable only on the topic index. The post index has
				// no title field: leave it unsorted (relevance truncation; core
				// re-sorts the merged list by topic title either way).
				return index === 'topic' ? [`title:${direction}`] : undefined;
			default:
				return undefined;
		}
	};
};

// Exposed so other direct-search call sites (lib/chat-search-global.js) get the same
// crash-proofing/degrade-and-alert behavior instead of duplicating or, worse, skipping it.
module.exports.safeSearch = safeSearch;
