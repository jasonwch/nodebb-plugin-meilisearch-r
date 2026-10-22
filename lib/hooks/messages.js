'use strict';

const winston = nodebb.require('winston');
const Messaging = nodebb.require('./src/messaging');
const { maybeDefer, meiliWrite } = require('../pending-queue');
const { sendDocuments } = require('../send-documents');
const { isFlagSet } = require('../flags');
const { encodeMid } = require('../id-codec');

// action:messaging.* hooks (save/edit/restore/delete) for the per-message chat index.
module.exports = function attachMessageHooks(plugin) {
	plugin.indexMessage = async function ({ message }) {
		// Park the mid only: the replay re-fetches via the fresh read below
		// (also handles purged/deleted-at-replay), avoiding stale-content
		// lost updates at the run->replay boundary.
		const deferPayload = (message && message.mid) ? { message: { mid: message.mid } } : { message };
		if (await maybeDefer(plugin, 'indexMessage', deferPayload)) return;
		if (!message || !message.mid) return;
		// F2: treat the DB as ground truth and REPLACE the payload entirely (same
		// pattern as indexPost). Hook payloads stringify roomId (REST sends pass it
		// from URL params; the edit path renders through getMessagesData which does
		// String(roomId)) — merging the payload over the fresh read indexed string
		// values into the filterable chat docs. Core writes the DB before firing
		// every action:messaging.* hook, so the fresh read always carries the newest
		// content/state, and getMessageFields int-parses mid/roomId/fromuid/timestamp.
		const fresh = await Messaging.getMessageFields(message.mid, ['mid', 'content', 'roomId', 'fromuid', 'timestamp', 'deleted', 'system']);
		// Bail if message was hard-purged mid-flight (fresh=null or stub with mid=0) — avoids indexing a phantom doc.
		if (!fresh || !fresh.mid) return;
		message = fresh;
		// Skip deleted messages (admin editing a deleted message should not re-index it).
		if (isFlagSet(message.deleted)) {
			winston.debug(`[plugin/meilisearch] Skipping deleted message ${message.mid}`);
			return;
		}
		// Skip system messages (join/leave/rename etc. — no searchable content, mirrors dbsearch).
		if (isFlagSet(message.system)) {
			winston.debug(`[plugin/meilisearch] Skipping system message ${message.mid}`);
			return;
		}
		// Defensive numeric coercion of the filterable/sortable fields — parse-or-
		// original: parseInt returns NaN for non-numeric values, and federated chat
		// messages carry URL-shaped mids/fromuids which must be preserved verbatim
		// (F2 follow-up). The mid itself additionally goes through encodeMid —
		// Meilisearch identifiers may only contain [a-zA-Z0-9_-], so a URL mid is
		// stored (reversibly) as "apm_" + base64url and decoded on every read path.
		const intOrOriginal = (value) => {
			if (value == null) return value;
			const parsed = parseInt(value, 10);
			return Number.isFinite(parsed) ? parsed : value;
		};
		const doc = {
			mid: encodeMid(intOrOriginal(message.mid)),
			roomId: intOrOriginal(message.roomId),
			uid: intOrOriginal(message.fromuid),
			content: message.content,
			timestamp: intOrOriginal(message.timestamp),
		};
		await meiliWrite(plugin, 'indexMessage', { message: { mid: message.mid } }, () =>
			sendDocuments(plugin, 'chat_message', [doc], { primaryKey: 'mid' }));
	};

	plugin.deindexMessage = async function ({ message }) {
		// Id-only park: the replay only needs the mid.
		const deferPayload = (message && message.mid) ? { message: { mid: message.mid } } : { message };
		if (await maybeDefer(plugin, 'deindexMessage', deferPayload)) return;
		if (!message || !message.mid) return;
		// Id-only fallback (bloat cleanup — deletes never go stale). The mid is
		// encoded: the index stores remote (URL) mids under their codec form.
		await meiliWrite(plugin, 'deindexMessage', { message: { mid: message.mid } }, () =>
			sendDocuments(plugin, 'chat_message', [encodeMid(message.mid)], { method: 'delete' }));
	};
};
