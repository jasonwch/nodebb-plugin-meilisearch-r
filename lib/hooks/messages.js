'use strict';

const winston = nodebb.require('winston');
const Messaging = nodebb.require('./src/messaging');
const { maybeDefer, meiliWrite } = require('../pending-queue');

// action:messaging.* hooks (save/edit/restore/delete) for the per-message chat index.
module.exports = function attachMessageHooks(plugin) {
	plugin.indexMessage = async function ({ message }) {
		const deferPayload = { message };
		if (await maybeDefer(plugin, 'indexMessage', deferPayload)) return;
		if (!message || !message.mid) return;
		// Re-fetch from DB if content/roomId/fromuid/timestamp missing or if this is an edit (payload may be partial).
		// Restore payload (delete.js) omits timestamp; save/edit payloads carry it. Re-fetch fills the gap.
		if (message.content === undefined || message.roomId === undefined || message.fromuid === undefined || message.timestamp === undefined) {
			const fresh = await Messaging.getMessageFields(message.mid, ['mid', 'content', 'roomId', 'fromuid', 'timestamp', 'deleted', 'system']);
			// Bail if message was hard-purged mid-flight (fresh=null or stub with mid=0) — avoids indexing a phantom doc.
			if (!fresh || !fresh.mid) return;
			message = { ...fresh, ...message };
		}
		// Skip deleted messages (admin editing a deleted message should not re-index it).
		if (parseInt(message.deleted, 10) === 1) {
			winston.debug(`[plugin/meilisearch] Skipping deleted message ${message.mid}`);
			return;
		}
		// Skip system messages (join/leave/rename etc. — no searchable content, mirrors dbsearch).
		if (parseInt(message.system, 10) === 1) {
			winston.debug(`[plugin/meilisearch] Skipping system message ${message.mid}`);
			return;
		}
		const doc = {
			mid: message.mid,
			roomId: message.roomId,
			uid: message.fromuid,
			content: message.content,
			timestamp: message.timestamp,
		};
		await meiliWrite(plugin, 'indexMessage', { message: { mid: message.mid } }, () =>
			plugin.client.index('chat_message').updateDocuments([doc], { primaryKey: 'mid' }));
	};

	plugin.deindexMessage = async function ({ message }) {
		const deferPayload = { message };
		if (await maybeDefer(plugin, 'deindexMessage', deferPayload)) return;
		if (!message || !message.mid) return;
		await meiliWrite(plugin, 'deindexMessage', { message }, () =>
			plugin.client.index('chat_message').deleteDocument(message.mid));
	};
};
