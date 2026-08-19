'use strict';

const winston = nodebb.require('winston');
const Topics = nodebb.require('./src/topics');
const batch = nodebb.require('./src/batch');
const { REINDEX_BATCH_SIZE } = require('../constants');
const { maybeDefer, meiliWrite, enqueuePending } = require('../pending-queue');

// action:topic.* hooks (post/save/edit/move/restore/delete/merge/changeOwner/purge)
// plus scheduled-topic publish handling.
module.exports = function attachTopicHooks(plugin) {
	plugin.indexTopic = async function ({ topic, tid, fromCid, post }) {
		const isMove = fromCid !== undefined;
		if (await maybeDefer(plugin, 'indexTopic', { topic, tid, fromCid, post })) return;
		// #2: On topic.move, no 'topic' key is provided — re-fetch by tid.
		if (!topic && tid) {
			topic = await Topics.getTopicData(tid);
		}
		if (!topic) {
			winston.warn('[plugin/meilisearch] indexTopic: no topic data');
			return;
		}
		// Issue 3: Skip deleted topics (admin editing/moving a deleted topic should not re-index it).
		if (parseInt(topic.deleted, 10) === 1) {
			winston.debug(`[plugin/meilisearch] Skipping deleted topic ${topic.tid}`);
			return;
		}
		// Issue 5: Skip scheduled topics (timestamp in future) — indexed on publish via onScheduledPublish.
		// action:topic.post fires AFTER action:post.save, so the main post may already be in Meili.
		// Deindex it here to prevent full-search from finding scheduled content prematurely.
		if (topic.scheduled || (topic.timestamp && topic.timestamp > Date.now())) {
			const mainPid = post ? post.pid : (parseInt(topic.mainPid, 10) > 0 ? topic.mainPid : null);
			if (mainPid) {
				await plugin.client.index('post').deleteDocument(mainPid).catch(() => {});
			}
			winston.debug(`[plugin/meilisearch] Skipping scheduled topic ${topic.tid}, deindexed main post ${mainPid || 'N/A'}`);
			return;
		}
		await meiliWrite(plugin, 'indexTopic', { topic, tid, fromCid, post }, () =>
			plugin.client.index('topic').updateDocuments([{
				tid: topic.tid,
				cid: topic.cid,
				uid: topic.uid,
				mainPid: topic.mainPid,
				title: topic.title,
				timestamp: topic.timestamp,
			}], { primaryKey: 'tid' }));
		// #4: On topic move, re-index child posts' cids (fire-and-forget with fallback).
		if (isMove && topic.tid) {
			if (plugin.healthy) {
				plugin.reindexTopicPosts({ tid: topic.tid }).catch(err =>
					winston.error(`[plugin/meilisearch] reindexTopicPosts failed: ${err.message}`));
			} else {
				await enqueuePending('reindexTopicPosts', { tid: topic.tid });
			}
		}
	};

	plugin.deindexTopic = async function ({ topic, tid }) {
		if (await maybeDefer(plugin, 'deindexTopic', { topic, tid })) return;
		// Tolerate topic.move-style payloads (no 'topic' key) — extract tid.
		const topicTid = topic ? topic.tid : tid;
		if (!topicTid) {
			winston.warn('[plugin/meilisearch] deindexTopic: no tid');
			return;
		}
		await meiliWrite(plugin, 'deindexTopic', { topic, tid }, () =>
			plugin.client.index('topic').deleteDocument(topicTid));
		// Also deindex the topic's posts from the post index so admin /search
		// (which queries both indexes and bypasses NodeBB's relevance-sort
		// deleted-content filter for admins) can't surface deleted-topic content.
		if (plugin.healthy) {
			plugin.deindexTopicPosts({ tid: topicTid }).catch(err =>
				winston.error(`[plugin/meilisearch] deindexTopicPosts failed: ${err.message}`));
		} else {
			await enqueuePending('deindexTopicPosts', { tid: topicTid });
		}
	};

	plugin.restoreTopic = async function ({ topic, tid }) {
		const topicTid = topic ? topic.tid : tid;
		if (await maybeDefer(plugin, 'restoreTopic', { topic, tid })) return;
		// Re-fetch topic if missing (defensive — same pattern as indexTopic).
		if (!topic && topicTid) {
			topic = await Topics.getTopicData(topicTid);
		}
		if (!topic) {
			winston.warn('[plugin/meilisearch] restoreTopic: no topic data');
			return;
		}
		// Re-index the topic document.
		await meiliWrite(plugin, 'restoreTopic', { topic, tid }, () =>
			plugin.client.index('topic').updateDocuments([{
				tid: topic.tid,
				cid: topic.cid,
				uid: topic.uid,
				mainPid: topic.mainPid,
				title: topic.title,
				timestamp: topic.timestamp,
			}], { primaryKey: 'tid' }));
		// Re-index the topic's posts (they were deindexed on delete).
		if (plugin.healthy) {
			plugin.reindexTopicPosts({ tid: topic.tid }).catch(err =>
				winston.error(`[plugin/meilisearch] restoreTopic reindexTopicPosts failed: ${err.message}`));
		} else {
			await enqueuePending('reindexTopicPosts', { tid: topic.tid });
		}
	};

	plugin.changeTopicOwner = async function ({ topics, toUid }) {
		if (await maybeDefer(plugin, 'changeTopicOwner', { topics, toUid })) return;
		// topics have OLD uid — re-fetch from DB to get fresh uid.
		// Skip deleted or scheduled topics.
		const validTids = (Array.isArray(topics) ? topics : [])
			.filter(t => t && t.tid && parseInt(t.deleted, 10) !== 1)
			.map(t => t.tid);
		if (!validTids.length) return;
		const freshTopics = await Topics.getTopicsFields(validTids, ['tid', 'cid', 'uid', 'mainPid', 'title', 'timestamp']);
		// Fix 7: Skip scheduled topics (timestamp in future).
		const activeTopics = freshTopics.filter(t => t && !(t.timestamp && t.timestamp > Date.now()));
		if (!activeTopics.length) return;
		const docs = activeTopics.map(topic => ({
			tid: topic.tid,
			cid: topic.cid,
			uid: topic.uid,
			mainPid: topic.mainPid,
			title: topic.title,
			timestamp: topic.timestamp,
		}));
		await meiliWrite(plugin, 'changeTopicOwner', { topics: activeTopics.map(t => ({ tid: t.tid })), toUid }, () =>
			plugin.client.index('topic').updateDocuments(docs, { primaryKey: 'tid' }));
	};

	plugin.onTopicMerge = async function ({ otherTids, mergeIntoTid }) {
		if (await maybeDefer(plugin, 'onTopicMerge', { otherTids, mergeIntoTid })) return;
		const postIndex = plugin.client.index('post');
		// Deindex merged-out topics + their posts (Topics.delete in merge fires no action:topic.delete).
		for (const tid of otherTids) {
			await plugin.deindexTopic({ tid });
			// Merge zeros mainPid before Topics.delete, so deindexTopicPosts can't find the main post.
			// Fallback: search Meili for any orphaned posts still carrying this tid.
			try {
				const result = await postIndex.search('', { filter: `tid = ${parseInt(tid, 10)}`, limit: 1000 });
				if (result.hits && result.hits.length) {
					const orphanPids = result.hits.map(h => h.pid);
					await meiliWrite(plugin, 'deindexPostsPurge', { posts: orphanPids.map(pid => ({ pid })) }, () =>
						postIndex.deleteDocuments(orphanPids));
				}
			} catch (err) {
				// B2b: SDK write error path — may contain document content snippets from
				// already-indexed posts (rare; only if Meilisearch rejects a batch quoting
				// the bad doc). Accepted risk for low-frequency write failures; document
				// content is already-in-DB data.
				winston.error(`[plugin/meilisearch] onTopicMerge orphan cleanup for tid ${tid} failed: ${err.message}`);
				plugin.healthy = false;
				// Don't enqueue — orphans are cleaned by next reindex. Continue to next tid.
			}
		}
		// Re-index the merge target (postcount/lastpost changed; posts already moved via action:post.move).
		await plugin.indexTopic({ tid: mergeIntoTid });
		if (plugin.healthy) {
			plugin.reindexTopicPosts({ tid: mergeIntoTid }).catch(err =>
				winston.error(`[plugin/meilisearch] onTopicMerge reindexTopicPosts failed: ${err.message}`));
		}
	};

	plugin.onScheduledPublish = async function ({ topics }) {
		const tids = (Array.isArray(topics) ? topics : []).filter(t => t && t.tid).map(t => t.tid);
		if (!tids.length) return;
		if (await maybeDefer(plugin, 'onScheduledPublish', { topics: tids.map(tid => ({ tid })) })) return;
		// Re-index each published topic + its posts (DB deleted=0, timestamp in past now).
		for (const tid of tids) {
			await plugin.indexTopic({ tid });
			if (plugin.healthy) {
				plugin.reindexTopicPosts({ tid }).catch(err =>
					winston.error(`[plugin/meilisearch] onScheduledPublish reindexTopicPosts failed: ${err.message}`));
			}
		}
	};

	plugin.deindexTopicsPurge = async function ({ topics }) {
		if (await maybeDefer(plugin, 'deindexTopicsPurge', { topics })) return;
		const topicsArr = Array.isArray(topics) ? topics.filter(t => t && t.tid) : [];
		if (!topicsArr.length) {
			return;
		}
		const tids = topicsArr.map(t => t.tid);
		const mainPids = topicsArr.map(t => t.mainPid).filter(Boolean);
		const postIndex = plugin.client.index('post');
		const reducedPayload = { topics: topicsArr.map(t => ({ tid: t.tid, mainPid: t.mainPid })) };
		try {
			await Promise.all([
				plugin.client.index('topic').deleteDocuments(tids),
				mainPids.length ? postIndex.deleteDocuments(mainPids) : Promise.resolve(),
			]);
			await Promise.all(topicsArr.map(async (t) => {
				await batch.processSortedSet(
					`tid:${t.tid}:posts`,
					async (pids) => {
						if (!pids.length) {
							return;
						}
						await postIndex.deleteDocuments(pids);
					},
					{ batch: REINDEX_BATCH_SIZE },
				);
			}));
		} catch (err) {
			// B2b: SDK write error path — may contain document content snippets from
			// already-indexed posts (rare; only if Meilisearch rejects a batch quoting
			// the bad doc). Accepted risk for low-frequency write failures; document
			// content is already-in-DB data.
			winston.error(`[plugin/meilisearch] deindexTopicsPurge failed: ${err.message}`);
			plugin.healthy = false;
			await enqueuePending('deindexTopicsPurge', reducedPayload);
		}
	};
};
