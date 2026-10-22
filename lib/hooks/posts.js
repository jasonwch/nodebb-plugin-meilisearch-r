'use strict';

const winston = nodebb.require('winston');
const Posts = nodebb.require('./src/posts');
const Topics = nodebb.require('./src/topics');
const batch = nodebb.require('./src/batch');
const { REINDEX_BATCH_SIZE } = require('../constants');
const { maybeDefer, meiliWrite, enqueuePending } = require('../pending-queue');
const { sendDocuments } = require('../send-documents');
const { isFlagSet } = require('../flags');
const { encodePid } = require('../id-codec');

// action:post.* hooks (save/edit/move/restore/delete/changeOwner/purge) plus the
// topic-level post reindex/deindex helpers used when a topic itself moves or is deleted.
module.exports = function attachPostHooks(plugin) {
	plugin.indexPost = async function ({ post, tid: newTid }) {
		// Park ids only: a full content snapshot replays stale — a doc edited again
		// after running=false is published (direct write, task N) would be overwritten
		// by the older parked copy when the replay lands later (task N+1, FIFO last-
		// wins). The replay re-fetches fresh content via the partial-payload branch
		// below, which also handles deleted/purged-at-replay.
		const deferPayload = (post && post.pid) ? { post: { pid: post.pid }, tid: newTid } : { post, tid: newTid };
		if (await maybeDefer(plugin, 'indexPost', deferPayload)) return;
		// #3: On post.move, post.tid is stale and content may be missing.
		// Re-fetch to get fresh tid + content from DB (already updated before action hook fires).
		// The deleted flag too: some payloads carry it as a BOOLEAN (action:post.edit
		// fires with deleted = !!deleted — src/posts/edit.js) and some omit it
		// entirely — the fresh DB read is the ground truth either way, so a
		// soft-deleted post can never be re-indexed by an edit.
		if (newTid !== undefined || !post || post.content === undefined || post.deleted === undefined) {
			if (post && post.pid) {
				post = await Posts.getPostData(post.pid);
			}
			if (!post) return;
		}
		// Issue 3: Skip deleted posts (admin editing a deleted post should not re-index it).
		if (isFlagSet(post.deleted)) {
			winston.debug(`[plugin/meilisearch] Skipping deleted post ${post.pid}`);
			return;
		}
		// Point 1: Skip posts in deleted or scheduled topics.
		const topicData = await Topics.getTopicFields(post.tid, ['deleted', 'timestamp']);
		if (parseInt(topicData?.deleted, 10) === 1) {
			winston.debug(`[plugin/meilisearch] Skipping post ${post.pid} in deleted topic ${post.tid}`);
			return;
		}
		if (topicData?.timestamp && topicData.timestamp > Date.now()) {
			winston.debug(`[plugin/meilisearch] Skipping post ${post.pid} in scheduled topic ${post.tid}`);
			return;
		}
		if (!post.cid) {
			post.cid = await Posts.getCidByPid(post.pid);
		}
		const doc = {
			// remote (URL) pids stored under their reversible codec form (id-codec)
			pid: encodePid(post.pid),
			tid: post.tid,
			cid: post.cid,
			uid: post.uid,
			content: post.content,
			timestamp: post.timestamp,
		};
		await meiliWrite(plugin, 'indexPost', { post: { pid: post.pid } }, () =>
			sendDocuments(plugin, 'post', [doc], { primaryKey: 'pid' }));
	};

	plugin.deindexPost = async function ({ post }) {
		// Id-only park: the replay only needs the pid, and the failure-path
		// re-enqueue then stays small too.
		if (await maybeDefer(plugin, 'deindexPost', (post && post.pid) ? { post: { pid: post.pid } } : { post })) return;
		// Id-only fallback (bloat cleanup — deletes never go stale). The pid is
		// encoded: the index stores remote (URL) pids under their codec form.
		await meiliWrite(plugin, 'deindexPost', { post: { pid: post.pid } }, () =>
			sendDocuments(plugin, 'post', [encodePid(post.pid)], { method: 'delete' }));
	};

	plugin.changePostOwner = async function ({ posts, toUid }) {
		// H2: park ids only. Full post snapshots (content included) made one bulk
		// changeOwner op one multi-MB zset member — Mongo rejects those outright at
		// the 16MB BSON cap (losing the deferral entirely), and every adapter pays a
		// proportional JSON.parse at drain time. The replay re-fetches fresh data
		// below, so only the ids are needed (same shape the meiliWrite failure path
		// below has always parked).
		if (await maybeDefer(plugin, 'changePostOwner', {
			posts: (Array.isArray(posts) ? posts : []).filter(p => p && p.pid).map(p => ({ pid: p.pid })),
			toUid,
		})) return;
		// posts have OLD uid — re-fetch from DB to get fresh uid.
		// Skip deleted posts (changeOwner can be called on deleted posts).
		// H2: the deleted check moved to the FRESH read — id-only parked payloads no
		// longer carry the park-time flag, and the fresh read is the ground truth
		// either way (same pattern as indexPost).
		const validPids = (Array.isArray(posts) ? posts : [])
			.filter(p => p && p.pid)
			.map(p => p.pid);
		if (!validPids.length) return;
		const freshPosts = await Posts.getPostsFields(validPids, ['pid', 'tid', 'uid', 'content', 'timestamp', 'deleted']);
		// Fix 7: Skip posts in deleted or scheduled topics.
		const tids = [...new Set(freshPosts.map(p => p && p.tid).filter(Boolean))];
		const topicsData = tids.length ? await Topics.getTopicsFields(tids, ['tid', 'deleted', 'timestamp']) : [];
		const tidToDeleted = {};
		const tidToScheduled = {};
		topicsData.forEach((t) => {
			tidToDeleted[t.tid] = parseInt(t.deleted, 10) === 1;
			tidToScheduled[t.tid] = t.timestamp && t.timestamp > Date.now();
		});
		const activePosts = freshPosts.filter(post => post && post.pid && !isFlagSet(post.deleted) &&
			!tidToDeleted[post.tid] && !tidToScheduled[post.tid]);
		if (!activePosts.length) return;
		const activePids = activePosts.map(p => p.pid);
		const cids = await Posts.getCidsByPids(activePids);
		const docs = activePosts.map((post, index) => ({
			pid: encodePid(post.pid),
			tid: post.tid,
			cid: cids[index],
			uid: post.uid,
			content: post.content,
			timestamp: post.timestamp,
		}));
		await meiliWrite(plugin, 'changePostOwner', { posts: activePids.map(pid => ({ pid })), toUid }, () =>
			sendDocuments(plugin, 'post', docs, { primaryKey: 'pid' }));
	};

	plugin.deindexPostsPurge = async function ({ posts }) {
		// H2: park ids only (see changePostOwner) — the payload from core carries
		// full post objects including content.
		if (await maybeDefer(plugin, 'deindexPostsPurge', {
			posts: (Array.isArray(posts) ? posts : []).map(p => p && p.pid).filter(Boolean).map(pid => ({ pid })),
		})) return;
		const pids = Array.isArray(posts) ? posts.map(p => p && p.pid).filter(Boolean) : [];
		if (!pids.length) {
			return;
		}
		const reducedPayload = { posts: pids.map(pid => ({ pid })) };
		// pids are DB-form ids (encoded at send time): remote (URL) pids live in
		// the index under their codec form.
		await meiliWrite(plugin, 'deindexPostsPurge', reducedPayload, () =>
			sendDocuments(plugin, 'post', pids.map(encodePid), { method: 'delete' }));
	};

	plugin.reindexTopicPosts = async function ({ tid }) {
		if (!tid) return;
		try {
			// Bug A: mainPid is NOT in tid:${tid}:posts — index it separately.
			const mainPid = await Topics.getTopicField(tid, 'mainPid');
			if (mainPid) {
				const [mainPost] = await Posts.getPostsFields([mainPid], ['pid', 'tid', 'uid', 'content', 'timestamp', 'deleted']);
				if (mainPost && parseInt(mainPost.deleted, 10) !== 1) {
					const [mainCid] = await Posts.getCidsByPids([mainPid]);
					await sendDocuments(plugin, 'post', [{
						pid: encodePid(mainPost.pid),
						tid: mainPost.tid,
						cid: mainCid,
						uid: mainPost.uid,
						content: mainPost.content,
						timestamp: mainPost.timestamp,
					}], { primaryKey: 'pid' });
				}
			}
			// Bug B: skip individually-deleted posts in the batch loop.
			await batch.processSortedSet(
				`tid:${tid}:posts`,
				async (pids) => {
					if (!pids.length) return;
					const posts = await Posts.getPostsFields(pids, ['pid', 'tid', 'uid', 'content', 'timestamp', 'deleted']);
					const activePosts = posts.filter(post => post && !isFlagSet(post.deleted));
					if (!activePosts.length) return;
					const activePids = activePosts.map(p => p.pid);
					const cids = await Posts.getCidsByPids(activePids);
					await sendDocuments(plugin, 'post',
						activePosts.map((post, index) => ({
							pid: encodePid(post.pid),
							tid: post.tid,
							cid: cids[index],
							uid: post.uid,
							content: post.content,
							timestamp: post.timestamp,
						})), { primaryKey: 'pid' });
				},
				{ batch: REINDEX_BATCH_SIZE },
			);
	} catch (err) {
		// B2b: SDK write error path — may contain document content snippets from already-indexed
		// posts (rare; only if Meilisearch rejects a batch quoting the bad doc). Accepted risk
		// for low-frequency write failures; document content is already-in-DB data.
		winston.error(`[plugin/meilisearch] reindexTopicPosts failed: ${err.message}`);
			plugin.healthy = false;
			await enqueuePending('reindexTopicPosts', { tid });
		}
	};

	plugin.deindexTopicPosts = async function ({ tid }) {
		if (!tid) return;
		try {
			// mainPid is NOT in tid:${tid}:posts — delete it separately. Remote
			// (URL) mainPids are encoded: the index stores them under codec form.
			const mainPid = await Topics.getTopicField(tid, 'mainPid');
			const mainPidArr = mainPid ? [mainPid] : [];
			await batch.processSortedSet(
				`tid:${tid}:posts`,
				async (pids) => {
					if (!pids.length) return;
					await sendDocuments(plugin, 'post', pids.map(encodePid), { method: 'delete' });
				},
				{ batch: REINDEX_BATCH_SIZE },
			);
			if (mainPidArr.length) {
				await sendDocuments(plugin, 'post', mainPidArr.map(encodePid), { method: 'delete' });
			}
		} catch (err) {
			// B2b: SDK write error path — may contain document content snippets from already-indexed
		// posts (rare; only if Meilisearch rejects a batch quoting the bad doc). Accepted risk
		// for low-frequency write failures; document content is already-in-DB data.
		winston.error(`[plugin/meilisearch] deindexTopicPosts failed: ${err.message}`);
			plugin.healthy = false;
			await enqueuePending('deindexTopicPosts', { tid });
		}
	};
};
