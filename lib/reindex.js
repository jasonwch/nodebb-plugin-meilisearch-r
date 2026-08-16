'use strict';

const _ = nodebb.require('lodash');
const winston = nodebb.require('winston');
const db = nodebb.require('./src/database');
const batch = nodebb.require('./src/batch');
const Posts = nodebb.require('./src/posts');
const Topics = nodebb.require('./src/topics');
const Messaging = nodebb.require('./src/messaging');
const pubsub = nodebb.require('./src/pubsub');
const settings = nodebb.require('./src/meta/settings');
const {
	REINDEX_BATCH_SIZE, REINDEX_LOCK_KEY, REINDEX_LOCK_TTL, LOCK_REFRESH_INTERVAL, REPLAY_OPS,
} = require('./constants');

// Full reindex of posts, topics, and chat messages, guarded by a cross-instance lock.
module.exports = function attachReindex(plugin) {
	const emitProgress = _.throttle(() => {
		pubsub.publish('meilisearch:reindex', plugin.indexing);
	}, 500, { leading: true, trailing: true });

	async function acquireReindexLock() {
		const ttl = await db.ttl(REINDEX_LOCK_KEY);
		if (ttl > 0) {
			winston.warn('[plugin/meilisearch] Reindex lock held by another instance');
			return false;
		}
		await db.set(REINDEX_LOCK_KEY, String(Date.now()));
		await db.expire(REINDEX_LOCK_KEY, REINDEX_LOCK_TTL);
		plugin.reindexLockRefresh = setInterval(() => {
			db.expire(REINDEX_LOCK_KEY, REINDEX_LOCK_TTL).catch(err =>
				winston.error(`[plugin/meilisearch] lock refresh failed: ${err.message}`));
		}, LOCK_REFRESH_INTERVAL);
		return true;
	}

	async function releaseReindexLock() {
		if (plugin.reindexLockRefresh) {
			clearInterval(plugin.reindexLockRefresh);
			plugin.reindexLockRefresh = null;
		}
		try {
			await db.delete(REINDEX_LOCK_KEY);
		} catch (err) {
			winston.warn(`[plugin/meilisearch] Could not release reindex lock: ${err.message}`);
		}
	}

	async function runReindex(force) {
		let succeeded = false;
		let errorMsg = null;
		let skippedDeletedTopics = 0;
		let skippedDeletedPosts = 0;
		let skippedDeletedMessages = 0;
		let skippedSystemMessages = 0;
		let skippedOrphanMessages = 0;
		const locked = await acquireReindexLock();
		if (!locked) {
			plugin.indexing.running = false;
			return;
		}
		pubsub.publish('meilisearch:init', true);
		plugin.initializingOnAnotherInstance = true;
		try {
			if (force) {
				plugin.reindexingForced = true;
				const postTask = await plugin.client.index('post').deleteAllDocuments();
				const topicTask = await plugin.client.index('topic').deleteAllDocuments();
				const messageTask = await plugin.client.index('chat_message').deleteAllDocuments();
				await plugin.client.tasks.waitForTask(postTask.taskUid, { timeout: 600000, interval: 200 });
				await plugin.client.tasks.waitForTask(topicTask.taskUid, { timeout: 600000, interval: 200 });
				await plugin.client.tasks.waitForTask(messageTask.taskUid, { timeout: 600000, interval: 200 });
			}
			const [postSettingsTask, topicSettingsTask, messageSettingsTask] = await plugin.updateIndexSettings();
			await plugin.client.tasks.waitForTask(postSettingsTask.taskUid, { timeout: 600000, interval: 200 });
			await plugin.client.tasks.waitForTask(topicSettingsTask.taskUid, { timeout: 600000, interval: 200 });
			await plugin.client.tasks.waitForTask(messageSettingsTask.taskUid, { timeout: 600000, interval: 200 });
			plugin.indexing.topic_progress.total = await db.sortedSetCard('topics:tid') || 0;
			plugin.indexing.post_progress.total = await db.sortedSetCard('posts:pid') || 0;
			plugin.indexing.message_progress.total = await db.sortedSetCard('messages:mid') || 0;
			emitProgress();
			await Promise.all([
				batch.processSortedSet(
					'topics:tid',
					async (tids) => {
						plugin.indexing.topic_progress.current += tids.length;
						emitProgress();
						const topics = await Topics.getTopicsFields(tids, ['tid', 'cid', 'uid', 'mainPid', 'title', 'timestamp', 'deleted']);
						const activeTopics = [];
						const deletedTids = [];
						topics.forEach((topic) => {
							if (parseInt(topic.deleted, 10) === 1) {
								deletedTids.push(topic.tid);
							} else {
								activeTopics.push(topic);
							}
						});
						skippedDeletedTopics += deletedTids.length;
						if (deletedTids.length) {
							await plugin.client.index('topic').deleteDocuments(deletedTids);
						}
						if (!activeTopics.length) return;
						await plugin.client.index('topic').updateDocuments(
							activeTopics.map(topic => ({
								tid: topic.tid,
								cid: topic.cid,
								uid: topic.uid,
								mainPid: topic.mainPid,
								title: topic.title,
								timestamp: topic.timestamp,
							})),
							{ primaryKey: 'tid' },
						);
					},
					{
						batch: REINDEX_BATCH_SIZE,
						progress: plugin.indexing.topic_progress,
					},
				),
				batch.processSortedSet(
					'posts:pid',
					async (pids) => {
						plugin.indexing.post_progress.current += pids.length;
						emitProgress();
						const posts = await Posts.getPostsFields(pids, ['pid', 'tid', 'uid', 'content', 'timestamp', 'deleted']);
						// Fetch topic deleted status for posts in this batch
						const tids = [...new Set(posts.map(p => p && p.tid).filter(Boolean))];
						const topicsData = tids.length ? await Topics.getTopicsFields(tids, ['tid', 'deleted']) : [];
						const tidToDeleted = {};
						topicsData.forEach((t) => { tidToDeleted[t.tid] = parseInt(t.deleted, 10) === 1; });

						const activePosts = [];
						const deletedPids = [];
						posts.forEach((post) => {
							if (parseInt(post.deleted, 10) === 1 || tidToDeleted[post.tid]) {
								deletedPids.push(post.pid);
							} else {
								activePosts.push(post);
							}
						});
						skippedDeletedPosts += deletedPids.length;
						if (deletedPids.length) {
							await plugin.client.index('post').deleteDocuments(deletedPids);
						}
						const activePids = activePosts.map(p => p.pid);
						const cids = activePids.length ? await Posts.getCidsByPids(activePids) : [];
						await plugin.client.index('post').updateDocuments(
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
					{
						batch: REINDEX_BATCH_SIZE,
						progress: plugin.indexing.post_progress,
					},
				),
			]);
			// Reindex chat messages SEQUENTIALLY after topics+posts (keeps Meilisearch enqueue rate bounded).
			// Option B: cross-check each batch's roomIds against existing rooms and deindex orphan messages
			// whose room was deleted via admin (messaging.deleteRooms fires no message hook and leaves
			// message:${mid} objects + messages:mid entries, so they would otherwise be re-indexed as orphans).
			await batch.processSortedSet(
				'messages:mid',
				async (mids) => {
					plugin.indexing.message_progress.current += mids.length;
					emitProgress();
					const messages = await Messaging.getMessagesFields(mids, ['mid', 'content', 'roomId', 'fromuid', 'timestamp', 'deleted', 'system']);
					// Resolve deleted/system messages and orphan rooms in one pass.
					const seenRoomIds = [...new Set(messages.map(m => m && m.roomId).filter(rid => rid != null && rid !== ''))];
					const roomsData = seenRoomIds.length ? await Messaging.getRoomsData(seenRoomIds) : [];
					const roomIdToExists = {};
					roomsData.forEach((room) => {
						if (room && room.roomId != null) roomIdToExists[room.roomId] = true;
					});
					const activeMessages = [];
					const deletedMids = [];
					const systemMids = [];
					const orphanMids = [];
					messages.forEach((msg) => {
						if (!msg || !msg.mid) return;
						if (parseInt(msg.deleted, 10) === 1) {
							deletedMids.push(msg.mid);
						} else if (parseInt(msg.system, 10) === 1) {
							systemMids.push(msg.mid);
						} else if (!roomIdToExists[msg.roomId]) {
							orphanMids.push(msg.mid);
						} else {
							activeMessages.push(msg);
						}
					});
					skippedDeletedMessages += deletedMids.length;
					skippedSystemMessages += systemMids.length;
					skippedOrphanMessages += orphanMids.length;
					const allDeindexMids = [...deletedMids, ...systemMids, ...orphanMids];
					if (allDeindexMids.length) {
						await plugin.client.index('chat_message').deleteDocuments(allDeindexMids);
					}
					if (!activeMessages.length) return;
					await plugin.client.index('chat_message').updateDocuments(
						activeMessages.map(msg => ({
							mid: msg.mid,
							roomId: msg.roomId,
							uid: msg.fromuid,
							content: msg.content,
							timestamp: msg.timestamp,
						})),
						{ primaryKey: 'mid' },
					);
				},
				{ batch: REINDEX_BATCH_SIZE, progress: plugin.indexing.message_progress },
			);
			succeeded = true;
			winston.info('[plugin/meilisearch] Indexing of posts, topics and chat messages complete');
		} catch (err) {
			winston.error(`[plugin/meilisearch] Indexing failed: ${err.message}`);
			errorMsg = err.message;
		} finally {
			plugin.indexing.running = false;
			plugin.reindexingForced = false;
			plugin.initializingOnAnotherInstance = false;
			pubsub.publish('meilisearch:init', false);
			await releaseReindexLock();
			emitProgress.flush();
			const pending = plugin.pendingDuringReindex.splice(0);
			for (const { op, payload } of pending) {
				if (!REPLAY_OPS.includes(op)) continue;
				try {
					await plugin[op](payload);
				} catch (err) {
					winston.error(`[plugin/meilisearch] replay ${op} failed: ${err.message}`);
				}
			}
			const lastIndexResult = {
				success: succeeded,
				finishedAt: Date.now(),
				topic_progress: {
					current: plugin.indexing.topic_progress.current,
					total: plugin.indexing.topic_progress.total,
				},
				post_progress: {
					current: plugin.indexing.post_progress.current,
					total: plugin.indexing.post_progress.total,
				},
				message_progress: {
					current: plugin.indexing.message_progress.current,
					total: plugin.indexing.message_progress.total,
				},
				skippedDeletedTopics,
				skippedDeletedPosts,
				skippedDeletedMessages,
				skippedSystemMessages,
				skippedOrphanMessages,
				error: errorMsg,
			};
			try {
				await settings.set(plugin.id, { lastReindexResult: lastIndexResult }, true);
				if (succeeded) {
					await settings.set(plugin.id, { indexed: true }, true);
				}
			} catch (err) {
				winston.error(`[plugin/meilisearch] Failed to persist reindex result: ${err.message}`);
			}
		}
	}

	plugin.reindex = async function (force = false) {
		if (plugin.indexing.running) {
			winston.warn('[plugin/meilisearch] Already indexing');
			return;
		}
		plugin.indexing = {
			running: true,
			topic_progress: {
				current: 0,
				total: 0,
			},
			post_progress: {
				current: 0,
				total: 0,
			},
			message_progress: {
				current: 0,
				total: 0,
			},
		};
		emitProgress();
		winston.info(`[plugin/meilisearch] Indexing posts, topics and chat messages${force ? ' (forced)' : ''}`);
		plugin._reindexPromise = runReindex(force);
		return plugin._reindexPromise;
	};
};
