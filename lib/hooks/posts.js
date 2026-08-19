'use strict';

const winston = nodebb.require('winston');
const Posts = nodebb.require('./src/posts');
const Topics = nodebb.require('./src/topics');
const batch = nodebb.require('./src/batch');
const { REINDEX_BATCH_SIZE } = require('../constants');
const { maybeDefer, meiliWrite, enqueuePending } = require('../pending-queue');

// action:post.* hooks (save/edit/move/restore/delete/changeOwner/purge) plus the
// topic-level post reindex/deindex helpers used when a topic itself moves or is deleted.
module.exports = function attachPostHooks(plugin) {
	plugin.indexPost = async function ({ post, tid: newTid }) {
		const deferPayload = { post, tid: newTid };
		if (await maybeDefer(plugin, 'indexPost', deferPayload)) return;
		// #3: On post.move, post.tid is stale and content may be missing.
		// Re-fetch to get fresh tid + content from DB (already updated before action hook fires).
		if (newTid !== undefined || !post || post.content === undefined) {
			if (post && post.pid) {
				post = await Posts.getPostData(post.pid);
			}
			if (!post) return;
		}
		// Issue 3: Skip deleted posts (admin editing a deleted post should not re-index it).
		if (parseInt(post.deleted, 10) === 1) {
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
			pid: post.pid,
			tid: post.tid,
			cid: post.cid,
			uid: post.uid,
			content: post.content,
			timestamp: post.timestamp,
		};
		await meiliWrite(plugin, 'indexPost', { post: { pid: post.pid } }, () =>
			plugin.client.index('post').updateDocuments([doc], { primaryKey: 'pid' }));
	};

	plugin.deindexPost = async function ({ post }) {
		if (await maybeDefer(plugin, 'deindexPost', { post })) return;
		await meiliWrite(plugin, 'deindexPost', { post }, () =>
			plugin.client.index('post').deleteDocument(post.pid));
	};

	plugin.changePostOwner = async function ({ posts, toUid }) {
		if (await maybeDefer(plugin, 'changePostOwner', { posts, toUid })) return;
		// posts have OLD uid — re-fetch from DB to get fresh uid.
		// Skip deleted posts (changeOwner can be called on deleted posts).
		const validPids = (Array.isArray(posts) ? posts : [])
			.filter(p => p && p.pid && parseInt(p.deleted, 10) !== 1)
			.map(p => p.pid);
		if (!validPids.length) return;
		const freshPosts = await Posts.getPostsFields(validPids, ['pid', 'tid', 'uid', 'content', 'timestamp']);
		// Fix 7: Skip posts in deleted or scheduled topics.
		const tids = [...new Set(freshPosts.map(p => p && p.tid).filter(Boolean))];
		const topicsData = tids.length ? await Topics.getTopicsFields(tids, ['tid', 'deleted', 'timestamp']) : [];
		const tidToDeleted = {};
		const tidToScheduled = {};
		topicsData.forEach((t) => {
			tidToDeleted[t.tid] = parseInt(t.deleted, 10) === 1;
			tidToScheduled[t.tid] = t.timestamp && t.timestamp > Date.now();
		});
		const activePosts = freshPosts.filter(post => post && !tidToDeleted[post.tid] && !tidToScheduled[post.tid]);
		if (!activePosts.length) return;
		const activePids = activePosts.map(p => p.pid);
		const cids = await Posts.getCidsByPids(activePids);
		const docs = activePosts.map((post, index) => ({
			pid: post.pid,
			tid: post.tid,
			cid: cids[index],
			uid: post.uid,
			content: post.content,
			timestamp: post.timestamp,
		}));
		await meiliWrite(plugin, 'changePostOwner', { posts: activePids.map(pid => ({ pid })), toUid }, () =>
			plugin.client.index('post').updateDocuments(docs, { primaryKey: 'pid' }));
	};

	plugin.deindexPostsPurge = async function ({ posts }) {
		if (await maybeDefer(plugin, 'deindexPostsPurge', { posts })) return;
		const pids = Array.isArray(posts) ? posts.map(p => p && p.pid).filter(Boolean) : [];
		if (!pids.length) {
			return;
		}
		const reducedPayload = { posts: pids.map(pid => ({ pid })) };
		await meiliWrite(plugin, 'deindexPostsPurge', reducedPayload, () =>
			plugin.client.index('post').deleteDocuments(pids));
	};

	plugin.reindexTopicPosts = async function ({ tid }) {
		if (!tid) return;
		const postIndex = plugin.client.index('post');
		try {
			// Bug A: mainPid is NOT in tid:${tid}:posts — index it separately.
			const mainPid = await Topics.getTopicField(tid, 'mainPid');
			if (mainPid) {
				const [mainPost] = await Posts.getPostsFields([mainPid], ['pid', 'tid', 'uid', 'content', 'timestamp', 'deleted']);
				if (mainPost && parseInt(mainPost.deleted, 10) !== 1) {
					const [mainCid] = await Posts.getCidsByPids([mainPid]);
					await postIndex.updateDocuments([{
						pid: mainPost.pid,
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
					const activePosts = posts.filter(post => post && parseInt(post.deleted, 10) !== 1);
					if (!activePosts.length) return;
					const activePids = activePosts.map(p => p.pid);
					const cids = await Posts.getCidsByPids(activePids);
					await postIndex.updateDocuments(
						activePosts.map((post, index) => ({
							pid: post.pid,
							tid: post.tid,
							cid: cids[index],
							uid: post.uid,
							content: post.content,
							timestamp: post.timestamp,
						})),
						{ primaryKey: 'pid' },
					);
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
		const postIndex = plugin.client.index('post');
		try {
			// mainPid is NOT in tid:${tid}:posts — delete it separately.
			const mainPid = await Topics.getTopicField(tid, 'mainPid');
			const mainPidArr = mainPid ? [mainPid] : [];
			await batch.processSortedSet(
				`tid:${tid}:posts`,
				async (pids) => {
					if (!pids.length) return;
					await postIndex.deleteDocuments(pids);
				},
				{ batch: REINDEX_BATCH_SIZE },
			);
			if (mainPidArr.length) {
				await postIndex.deleteDocuments(mainPidArr);
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
