'use strict';

const winston = nodebb.require('winston');
const {
	MEILI_SEND_RETRIES, MEILI_SEND_RETRY_BASE_MS, MEILI_BISECT_CONSECUTIVE,
} = require('./constants');

// Single choke point for every document write to Meilisearch (reindex loops + live
// hooks). Strategy: optimistic full-batch POST, then self-heal:
//   - transient errors (network, timeout, 5xx, 429): retry the SAME payload a bounded
//     number of times with exponential backoff
//   - 413 payload-too-large: bisect (halve) the batch and retry each half recursively;
//     a single item that still 413s is quarantined (skipped + counted), never aborts
//     the run
//   - consecutive-bisect streak: after MEILI_BISECT_CONSECUTIVE batches in a row
//     needed bisection, adopt the last working size as that index's POST size
//     (in-memory, per-run only — the next run re-calibrates from the full batch size)
// The streak/size bookkeeping is PER INDEX (B2): the reindex runs the topics and posts
// loops concurrently under Promise.all, and topic documents are tiny (title-only), so
// topics batches rarely bisect — a shared streak would be constantly reset by topics
// successes and the posts batches' adaptive cap would never reliably engage. A shared
// lastWorkingSize would likewise be polluted by topics' full-size POSTs.
// Everything stays fire-and-forget: only the HTTP enqueue is awaited, never the
// Meilisearch task outcome (that is the drain monitor's job, lib/drain.js).

// SDK 0.59 error shapes (node_modules/meilisearch/dist/index.js):
//   MeilisearchApiError  — { response: fetch Response, cause: { code, message, type } }
//   MeilisearchRequestError / MeilisearchRequestTimeOutError — network layer failures
function httpStatus(err) {
	if (err && err.response && typeof err.response.status === 'number') {
		return err.response.status;
	}
	return null;
}

function isPayloadTooLarge(err) {
	if (httpStatus(err) === 413) return true;
	const code = (err && err.cause && err.cause.code) || (err && err.code);
	if (code === 'payload_too_large') return true;
	return /payload (is )?too large/i.test(String((err && err.message) || ''));
}

function isTransient(err) {
	if (!err) return false;
	if (err.name === 'MeilisearchRequestError' ||
		err.name === 'MeilisearchRequestTimeOutError' ||
		err.name === 'TypeError') {
		return true;
	}
	const status = httpStatus(err);
	return status === 429 || (status !== null && status >= 500);
}

const sleep = ms => new Promise(resolve => { setTimeout(resolve, ms); });

// Per-run state. byIndex (adaptive POST sizing) is keyed per index; everything the
// drain monitor consumes (task uids, totals, quarantine, live-deletion accounting)
// is run-global.
function freshSendState() {
	return {
		byIndex: {},           // indexUid -> { size, streak, lastWorkingSize }
		quarantined: 0,        // items skipped: single item exceeds the payload limit
		quarantinedByIndex: {}, // same, keyed by indexUid (for expected-count verification)
		enqueued: 0,           // document tasks successfully handed to Meilisearch
		firstTaskUid: null,
		lastTaskUid: null,
	};
}

function resetSendState(plugin) {
	plugin.meiliSend = freshSendState();
}

function getSendState(plugin) {
	if (!plugin.meiliSend) {
		resetSendState(plugin);
	}
	return plugin.meiliSend;
}

function getIndexState(state, indexUid) {
	if (!state.byIndex[indexUid]) {
		state.byIndex[indexUid] = { size: 0, streak: 0, lastWorkingSize: 0 };
	}
	return state.byIndex[indexUid];
}

function recordEnqueued(state, idxState, task, sliceLength) {
	state.enqueued += 1;
	idxState.lastWorkingSize = sliceLength;
	if (task && typeof task.taskUid === 'number') {
		// Min/max, not first/last resolution: the reindex's topics and posts loops run
		// concurrently, so POST responses can resolve in a different order than
		// Meilisearch assigned the uids. The drain monitor treats the max uid as the
		// FIFO drain sentinel (terminal max uid implies every earlier task finished)
		// and verification filters failed tasks by [first, last] — a resolution-order
		// value at either bound would both drain early (verification reading stale
		// counts => false missing-docs alarm) and hide failed tasks outside the range.
		state.firstTaskUid = state.firstTaskUid === null ? task.taskUid : Math.min(state.firstTaskUid, task.taskUid);
		state.lastTaskUid = state.lastTaskUid === null ? task.taskUid : Math.max(state.lastTaskUid, task.taskUid);
	}
}

async function postWithRetry(index, method, items, primaryKey) {
	let attempt = 0;
	for (;;) {
		try {
			return method === 'delete'
				? await index.deleteDocuments(items)
				: await index.updateDocuments(items, { primaryKey });
		} catch (err) {
			if (!isTransient(err) || attempt >= MEILI_SEND_RETRIES) {
				throw err;
			}
			const delay = MEILI_SEND_RETRY_BASE_MS * (2 ** attempt);
			winston.warn(`[plugin/meilisearch] transient Meilisearch ${method} error (${err.message}); retrying in ${delay}ms (attempt ${attempt + 1}/${MEILI_SEND_RETRIES})`);
			await sleep(delay);
			attempt += 1;
		}
	}
}

// Sends one slice; returns true if bisection was needed (streak accounting).
async function sendSlice(plugin, index, indexUid, method, slice, primaryKey, state, idxState) {
	try {
		const task = await postWithRetry(index, method, slice, primaryKey);
		recordEnqueued(state, idxState, task, slice.length);
		return false;
	} catch (err) {
		if (!isPayloadTooLarge(err)) {
			throw err;
		}
		if (slice.length === 1) {
			// Practically unreachable with NodeBB's ~32KB post cap against Meilisearch's
			// default 100MB limit — kept as the last-resort guard for exotic configs
			// (e.g. a deliberately tiny MEILI_HTTP_PAYLOAD_SIZE_LIMIT).
			state.quarantined += 1;
			state.quarantinedByIndex[indexUid] = (state.quarantinedByIndex[indexUid] || 0) + 1;
			const key = method === 'delete' ? slice[0] : slice[0][primaryKey];
			winston.warn(`[plugin/meilisearch] Quarantined ${method === 'delete' ? 'deletion of' : 'document'} ${key} on "${indexUid}": single item exceeds the Meilisearch payload limit`);
			return true;
		}
		const mid = Math.ceil(slice.length / 2);
		await sendSlice(plugin, index, indexUid, method, slice.slice(0, mid), primaryKey, state, idxState);
		await sendSlice(plugin, index, indexUid, method, slice.slice(mid), primaryKey, state, idxState);
		return true;
	}
}

// Send documents (method 'update', docs are objects) or deletions (method 'delete',
// docs are primary-key values) to an index with retry + bisection hardening.
async function sendDocuments(plugin, indexUid, docs, options = {}) {
	const { primaryKey, method = 'update' } = options;
	if (!Array.isArray(docs) || !docs.length) {
		return;
	}
	const state = getSendState(plugin);
	const index = plugin.client.index(indexUid);
	const idxState = getIndexState(state, indexUid);
	const size = idxState.size > 0 ? Math.min(idxState.size, docs.length) : docs.length;
	let neededBisect = false;
	for (let offset = 0; offset < docs.length; offset += size) {
		const slice = docs.slice(offset, offset + size);
		neededBisect = (await sendSlice(plugin, index, indexUid, method, slice, primaryKey, state, idxState)) || neededBisect;
	}
	idxState.streak = neededBisect ? idxState.streak + 1 : 0;
	if (idxState.streak >= MEILI_BISECT_CONSECUTIVE && idxState.lastWorkingSize > 1) {
		idxState.size = Math.max(1, idxState.lastWorkingSize);
		idxState.streak = 0;
		winston.warn(`[plugin/meilisearch] Payload limit hit on ${MEILI_BISECT_CONSECUTIVE} consecutive "${indexUid}" batches; capping POST size at ${idxState.size} item(s) for the remainder of this run`);
	}
}

module.exports = {
	sendDocuments,
	resetSendState,
	freshSendState,
	isPayloadTooLarge,
	isTransient,
};
