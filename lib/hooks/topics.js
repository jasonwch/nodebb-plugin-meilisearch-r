'use strict';

const winston = nodebb.require('winston');
const Topics = nodebb.require('./src/topics');
const batch = nodebb.require('./src/batch');
const { REINDEX_BATCH_SIZE } = require('../constants');
const { maybeDefer, meiliWrite, enqueuePending } = require('../pending-queue');
const { sendDocuments } = require('../send-documents');
const { isFlagSet } = require('../flags');
const { encodePid } = require('../id-codec');
const { filterValue } = require('../meili-filter');
const { waitForSucceededTask } = require('../client');

// action:topic.* hooks (post/save/edit/move/restore/delete/merge/changeOwner/purge)
// plus scheduled-topic publish handling.
module.exports = function attachTopicHooks(plugin) {
	plugin.indexTopic = async function ({ topic, tid, fromCid, post }) {
		const isMove = fromCid !== undefined;
		// Park the tid only (plus fromCid for the move case): the replay re-fetches
		// fresh topic data via the hook's own branch below, avoiding the stale-title
		// lost-update at the run->replay boundary. `post` is safe to drop — it only
		// feeds the scheduled-topic mainPid, which falls back to topic.mainPid.
		const deferTid = (topic && topic.tid) || tid;
		if (await maybeDefer(plugin, 'indexTopic', deferTid ? { tid: deferTid, fromCid } : { topic, tid, fromCid, post })) return;
		// #2: On topic.move, no 'topic' key is provided — re-fetch by tid. The
		// deleted flag too: action:topic.edit fires with a PARTIAL topic object
		// (no deleted field — src/posts/edit.js editMainPost), so re-fetch whenever
		// it's missing — a soft-deleted topic must never be re-indexed by a
		// main-post edit. (The action fires after db.setObject, so the fetched
		// data carries the new title too.)
		if ((!topic || topic.deleted === undefined) && deferTid) {
			topic = await Topics.getTopicData(deferTid);
		}
		if (!topic) {
			winston.warn('[plugin/meilisearch] indexTopic: no topic data');
			return;
		}
		// Issue 3: Skip deleted topics (admin editing/moving a deleted topic should not re-index it).
		if (isFlagSet(topic.deleted)) {
			winston.debug(`[plugin/meilisearch] Skipping deleted topic ${topic.tid}`);
			return;
		}
		// Issue 5: Skip scheduled topics (timestamp in future) — indexed on publish via onScheduledPublish.
		// action:topic.post fires AFTER action:post.save, so the main post may already be in Meili.
		// Deindex it here to prevent full-search from finding scheduled content prematurely.
		if (topic.scheduled || (topic.timestamp && topic.timestamp > Date.now())) {
			const mainPid = post ? post.pid : (parseInt(topic.mainPid, 10) > 0 ? topic.mainPid : null);
			if (mainPid) {
				await sendDocuments(plugin, 'post', [encodePid(mainPid)], { method: 'delete' }).catch(() => {});
			}
			// The topic doc itself: never indexed for newly-scheduled topics, but a
			// PUBLISHED topic rescheduled into the future WAS indexed — remove it
			// (a no-op in Meilisearch if absent), so unpublished titles are unsearchable.
			await sendDocuments(plugin, 'topic', [topic.tid], { method: 'delete' }).catch(() => {});
			winston.debug(`[plugin/meilisearch] Skipping scheduled topic ${topic.tid}, deindexed main post ${mainPid || 'N/A'}`);
			return;
		}
		// Id-only fallback: a failed write re-enqueues just the tid, so the retry
		// re-fetches fresh topic data above instead of replaying a stale title.
		await meiliWrite(plugin, 'indexTopic', { tid: topic.tid, fromCid }, () =>
			sendDocuments(plugin, 'topic', [{
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
		// Tolerate topic.move-style payloads (no 'topic' key) — extract tid.
		const topicTid = topic ? topic.tid : tid;
		// Id-only park on BOTH paths (reindex deferral and the unhealthy-Meili
		// fallback): the hook only needs topicTid for the delete — full topic
		// snapshots parked in the queue are pure bloat.
		if (await maybeDefer(plugin, 'deindexTopic', topicTid ? { tid: topicTid } : { topic, tid })) return;
		if (!topicTid) {
			winston.warn('[plugin/meilisearch] deindexTopic: no tid');
			return;
		}
		await meiliWrite(plugin, 'deindexTopic', { tid: topicTid }, () =>
			sendDocuments(plugin, 'topic', [topicTid], { method: 'delete' }));
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
		// Id-only park: the replay re-fetches fresh topic data below.
		if (await maybeDefer(plugin, 'restoreTopic', topicTid ? { tid: topicTid } : { topic, tid })) return;
		// Re-fetch topic if missing (defensive — same pattern as indexTopic).
		if (!topic && topicTid) {
			topic = await Topics.getTopicData(topicTid);
		}
		if (!topic) {
			winston.warn('[plugin/meilisearch] restoreTopic: no topic data');
			return;
		}
		// Re-index the topic document.
		// Id-only fallback: the retry re-fetches fresh topic data above.
		await meiliWrite(plugin, 'restoreTopic', { tid: topicTid }, () =>
			sendDocuments(plugin, 'topic', [{
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
		// H2: park ids only (see lib/hooks/posts.js changePostOwner) — full topic
		// snapshots made bulk ops multi-MB zset members (Mongo 16MB BSON cap).
		if (await maybeDefer(plugin, 'changeTopicOwner', {
			topics: (Array.isArray(topics) ? topics : []).filter(t => t && t.tid).map(t => ({ tid: t.tid })),
			toUid,
		})) return;
		// topics have OLD uid — re-fetch from DB to get fresh uid.
		// Skip deleted or scheduled topics.
		// H2: the deleted check moved to the FRESH read (id-only parked payloads no
		// longer carry the park-time flag).
		const validTids = (Array.isArray(topics) ? topics : [])
			.filter(t => t && t.tid)
			.map(t => t.tid);
		if (!validTids.length) return;
		const freshTopics = await Topics.getTopicsFields(validTids, ['tid', 'cid', 'uid', 'mainPid', 'title', 'timestamp', 'deleted']);
		// Fix 7: Skip scheduled topics (timestamp in future).
		const activeTopics = freshTopics.filter(t => t && !isFlagSet(t.deleted) && !(t.timestamp && t.timestamp > Date.now()));
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
			sendDocuments(plugin, 'topic', docs, { primaryKey: 'tid' }));
	};

	plugin.onTopicMerge = async function ({ otherTids, mergeIntoTid }) {
		if (await maybeDefer(plugin, 'onTopicMerge', { otherTids, mergeIntoTid })) return;
		if (!plugin.client) return;
		// Deindex merged-out topics + their posts (Topics.delete in merge fires no action:topic.delete).
		for (const tid of otherTids) {
			await plugin.deindexTopic({ tid });
			// F5: orphan sweep, uncapped. The previous search-then-delete loop read
			// with limit 1000, but Meilisearch caps EVERY read at pagination.maxTotalHits
			// (= maxDocuments) — merged-out topics carrying more orphans than that left
			// the excess indexed until the next force-reindex (live-verified: 600 ghost
			// docs, sweep deleted exactly 500). delete-by-filter bypasses read caps
			// entirely and removes every remaining post doc still carrying this tid
			// (moved posts' docs were already rewritten with the target tid by the
			// per-post action:post.move hooks fired during the merge, so they are not
			// matched by the filter).
			try {
				// filterValue: local tids stay bare numbers, remote (UUID) tids are
				// quoted string literals — parseInt here emitted `tid = NaN`, which
				// Meilisearch rejects as a filter syntax error.
				const enqueued = await plugin.client.index('post').deleteDocuments({ filter: `tid = ${filterValue(tid)}` });
				// Verify the task actually succeeded (a bad filter or a Meilisearch-side
				// rejection would otherwise be silent). A poll TIMEOUT is not a failure —
				// the deletion task is still running server-side.
				try {
					await waitForSucceededTask(plugin, enqueued.taskUid, { timeout: 60000, interval: 200 });
				} catch (err) {
					if (err.name !== 'MeilisearchTaskTimeOutError') throw err;
					winston.warn(`[plugin/meilisearch] Orphan cleanup task for tid ${tid} still running server-side (poll timeout) — continuing`);
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
		} else {
			// Meili went down mid-merge (the merge-target write failed and re-parked
			// itself): park the posts reindex too, or it is silently lost until the
			// next reindex — same pattern as restoreTopic/deindexTopic.
			await enqueuePending('reindexTopicPosts', { tid: mergeIntoTid });
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
			} else {
				// Meili went down mid-publish (indexTopic's write failed and re-parked
				// itself): park the posts reindex too, or it is silently lost until the
				// next reindex — same pattern as restoreTopic/deindexTopic.
				await enqueuePending('reindexTopicPosts', { tid });
			}
		}
	};

	plugin.deindexTopicsPurge = async function ({ topics }) {
		// H2: park ids only (see lib/hooks/posts.js changePostOwner). mainPid is kept:
		// the purge replay deletes the main posts by pid directly, since the
		// tid:{tid}:posts zsets are empty by the time the replay runs.
		if (await maybeDefer(plugin, 'deindexTopicsPurge', {
			topics: (Array.isArray(topics) ? topics : []).filter(t => t && t.tid)
				.map(t => ({ tid: t.tid, mainPid: t.mainPid })),
		})) return;
		const topicsArr = Array.isArray(topics) ? topics.filter(t => t && t.tid) : [];
		if (!topicsArr.length) {
			return;
		}
		const tids = topicsArr.map(t => t.tid);
		const mainPids = topicsArr.map(t => t.mainPid).filter(Boolean);
		const reducedPayload = { topics: topicsArr.map(t => ({ tid: t.tid, mainPid: t.mainPid })) };
		try {
			await Promise.all([
				sendDocuments(plugin, 'topic', tids, { method: 'delete' }),
				// mainPids encoded: remote (URL) main pids live under codec form.
				mainPids.length ? sendDocuments(plugin, 'post', mainPids.map(encodePid), { method: 'delete' }) : Promise.resolve(),
			]);
			await Promise.all(topicsArr.map(async (t) => {
				await batch.processSortedSet(
					`tid:${t.tid}:posts`,
					async (pids) => {
						if (!pids.length) {
							return;
						}
						await sendDocuments(plugin, 'post', pids.map(encodePid), { method: 'delete' });
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
