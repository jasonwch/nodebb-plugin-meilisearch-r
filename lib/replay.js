'use strict';

// Bulk replay for the simple id-scoped parked ops (see SIMPLE_OPS in
// lib/pending-queue.js). Each function mirrors its per-op hook's semantics
// exactly (lib/hooks/posts.js indexPost / lib/hooks/messages.js indexMessage —
// fresh fetch, deleted/system/purged-at-replay filtering) but sends ONE
// sendDocuments call for the whole group instead of one HTTP POST plus one
// fetch per op — the dominant replay-throughput win (1 POST/op -> 1 POST/group).

const Posts = nodebb.require('./src/posts');
const Topics = nodebb.require('./src/topics');
const Messaging = nodebb.require('./src/messaging');
const { sendDocuments } = require('./send-documents');
const { isFlagSet } = require('./flags');
const { encodeMid, encodePid } = require('./id-codec');

async function replayPostAdds(plugin, pids) {
	if (!pids.length) return;
	const posts = await Posts.getPostsFields(pids, ['pid', 'tid', 'uid', 'content', 'timestamp', 'deleted']);
	// Topic deleted/scheduled checks, bulk — same filters as indexPost.
	const tids = [...new Set(posts.map(p => p && p.tid).filter(Boolean))];
	const topicsData = tids.length ? await Topics.getTopicsFields(tids, ['tid', 'deleted', 'timestamp']) : [];
	const tidToDeleted = {};
	const tidToScheduled = {};
	topicsData.forEach((t) => {
		if (!t) return;
		tidToDeleted[t.tid] = parseInt(t.deleted, 10) === 1;
		tidToScheduled[t.tid] = t.timestamp && t.timestamp > Date.now();
	});
	const activePosts = posts.filter(post => post && post.pid &&
		!isFlagSet(post.deleted) && !tidToDeleted[post.tid] && !tidToScheduled[post.tid]);
	if (!activePosts.length) return;
	const activePids = activePosts.map(p => p.pid);
	const cids = await Posts.getCidsByPids(activePids);
	await sendDocuments(plugin, 'post',
		activePosts.map((post, index) => ({
			// remote (URL) pids stored under their reversible codec form (id-codec)
			pid: encodePid(post.pid),
			tid: post.tid,
			cid: cids[index],
			uid: post.uid,
			content: post.content,
			timestamp: post.timestamp,
		})), { primaryKey: 'pid' });
}

async function replayMessageAdds(plugin, mids) {
	if (!mids.length) return;
	const messages = await Messaging.getMessagesFields(mids, ['mid', 'content', 'roomId', 'fromuid', 'timestamp', 'deleted', 'system']);
	// Purged-at-replay (null/stub entries) and deleted/system filters — same as indexMessage.
	const activeMessages = messages.filter(msg => msg && msg.mid &&
		!isFlagSet(msg.deleted) && !isFlagSet(msg.system));
	if (!activeMessages.length) return;
	await sendDocuments(plugin, 'chat_message',
		activeMessages.map(msg => ({
			// remote (URL) mids stored under their reversible codec form (id-codec)
			mid: encodeMid(msg.mid),
			roomId: msg.roomId,
			uid: msg.fromuid,
			content: msg.content,
			timestamp: msg.timestamp,
		})), { primaryKey: 'mid' });
}

async function replayDeletes(plugin, index, ids) {
	if (!ids.length) return;
	// parked ids are DB-form ids — encode per index: chat mids and remote post
	// pids both live in their indexes under codec form, local numeric ids pass through
	const encode = index === 'chat_message' ? encodeMid : encodePid;
	const deleteIds = ids.map(encode);
	await sendDocuments(plugin, index, deleteIds, { method: 'delete' });
}

async function replayAdds(plugin, index, ids) {
	if (index === 'post') {
		return replayPostAdds(plugin, ids);
	}
	return replayMessageAdds(plugin, ids);
}

module.exports = { replayAdds, replayDeletes };
