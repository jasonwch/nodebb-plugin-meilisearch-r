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
	REINDEX_BATCH_SIZE, REINDEX_KEEPALIVE_MS, INDEXING_STALE_MS,
} = require('./constants');
const { isSemanticSearchEnabled } = require('./embedder');
const { waitForSucceededTask } = require('./client');
const { sendDocuments, resetSendState } = require('./send-documents');
const { startDrainMonitor, stopDrainMonitor, snapshotBacklog } = require('./drain');
const { drainAll, isReindexActive } = require('./pending-queue');
const { acquireReindexLock, releaseReindexLock, writeReindexHeartbeat, isReindexLockOwnedBy } = require('./lock');
const { isFlagSet } = require('./flags');
const { setJsonSetting } = require('./settings-json');
const { encodeMid, encodePid } = require('./id-codec');

// Full reindex of posts, topics, and chat messages, guarded by a cross-instance
// owner-token lock (lib/lock.js).
module.exports = function attachReindex(plugin) {
	const emitProgress = _.throttle(() => {
		pubsub.publish('meilisearch:reindex', plugin.indexing);
	}, 500, { leading: true, trailing: true });

	// Merged iteration over a LOCAL zset and its federated sibling (e.g.
	// posts:pid + postsRemote:pid): one logical loop per type, sequential per
	// set, sharing the caller-managed progress object. `batch.processSortedSet`
	// would overwrite progress.total with the CURRENT set's cardinality (it
	// assumes a solo set), so totals are summed by the caller and progress is
	// tracked by the batch callbacks themselves (current += ids.length) — the
	// `progress` option is deliberately NOT passed. Sequential-per-set keeps the
	// enqueue-rate profile identical to the pre-federation loops (two concurrent
	// type loops, chats after).
	async function processSetUnion(setKeys, process) {
		for (const setKey of setKeys) {
			await batch.processSortedSet(setKey, process, { batch: REINDEX_BATCH_SIZE });
		}
	}

	async function runReindex(force, options = {}) {
		let succeeded = false;
		let errorMsg = null;
		let skippedDeletedTopics = 0;
		let skippedDeletedPosts = 0;
		let skippedDeletedMessages = 0;
		let skippedSystemMessages = 0;
		let skippedOrphanMessages = 0;
		// Scheduled topics (deleted=0, future timestamp — they live in topics:tid
		// until publish): the reindex must not index them (nor their main posts),
		// and must remove any docs left over from a reschedule round trip.
		let skippedScheduledTopics = 0;
		let skippedScheduledPosts = 0;
		// Sorted-set entries whose object no longer exists (purged content leaves the
		// zset member behind). Counted so the drain verification's expected document
		// counts stay exact — and so posts/topics can never index a null primary key.
		let skippedMissingTopics = 0;
		let skippedMissingPosts = 0;
		let skippedMissingMessages = 0;
		// Snapshot of Meilisearch's task-queue backlog before Phase 1 starts (0 if the
		// run fails before the snapshot); the drain monitor subtracts it so foreign
		// tasks (snapshots, other instances' settings) are not counted as this run's.
		let baseBacklog = 0;
		const locked = await acquireReindexLock(plugin);
		if (!locked) {
			// Local flip only — nothing was ever published (running=true is broadcast
			// post-lock), so there is no phantom run to correct and no correction that
			// could clobber the winner's synced state. If another instance holds the
			// lock, its next progress publish resyncs this view (≤500ms mid-Phase-1,
			// ≤ keepalive interval during setup, before its batches POST). With a stale
			// lock there is no run at all and direct writes are correct.
			plugin.indexing.running = false;
			plugin.indexingUpdatedAt = Date.now();
			return;
		}
		// The ONLY broadcast of running=true — with the lock held. Arming cluster-wide
		// deferral here (rather than in plugin.reindex) is what makes lock-losers
		// phantom-free. The heartbeat: DB-backed liveness for the dead-holder reclaim
		// (lib/lock.js) and the false-self-heal gate (lib/pending-queue.js) — written
		// at acquisition and on every keepalive tick.
		await writeReindexHeartbeat();
		emitProgress();
		// Whole-run keepalive: the setup phase (awaited one-time deleteAll/settings
		// tasks, each with a 600s timeout) and any pathological batch can be silent far
		// longer than INDEXING_STALE_MS, which would make other instances — and this
		// one, via its own live traffic — falsely self-heal mid-run. This interval keeps
		// the pubsub-synced state fresh from lock acquisition until the finally.
		plugin.reindexKeepalive = setInterval(() => {
			plugin.indexingUpdatedAt = Date.now();
			emitProgress();
			writeReindexHeartbeat();
		}, REINDEX_KEEPALIVE_MS);
		if (plugin.reindexKeepalive.unref) {
			plugin.reindexKeepalive.unref();
		}
		pubsub.publish('meilisearch:init', true);
		plugin.initializingOnAnotherInstance = true;
		try {
			if (force) {
				plugin.reindexingForced = true;
				if (await isSemanticSearchEnabled(plugin)) {
					// Every document added back after this will need a fresh embedding call
					// against the configured (often paid) embedding API - there is no way
					// around that once the documents themselves are gone, so surface it here.
					winston.warn('[plugin/meilisearch] Force reindex with semantic search enabled: all vector embeddings will be regenerated from scratch via the embedding API.');
				}
				const postTask = await plugin.client.index('post').deleteAllDocuments();
				const topicTask = await plugin.client.index('topic').deleteAllDocuments();
				const messageTask = await plugin.client.index('chat_message').deleteAllDocuments();
				await waitForSucceededTask(plugin, postTask.taskUid, { timeout: 600000, interval: 200 });
				await waitForSucceededTask(plugin, topicTask.taskUid, { timeout: 600000, interval: 200 });
				await waitForSucceededTask(plugin, messageTask.taskUid, { timeout: 600000, interval: 200 });
			}
			// Claim-2 fix: options.settingsData carries the in-flight ACP save payload
			// when this reindex was fired by a host change (core persists the form only
			// AFTER the filter:settings.set hook returns) — without it these reads would
			// see the OLD pre-save values and push a stale embedder/keyword config.
			const [postSettingsTask, topicSettingsTask, messageSettingsTask] = await plugin.updateIndexSettings(options.settingsData);
			await waitForSucceededTask(plugin, postSettingsTask.taskUid, { timeout: 600000, interval: 200 });
			await waitForSucceededTask(plugin, topicSettingsTask.taskUid, { timeout: 600000, interval: 200 });
			await waitForSucceededTask(plugin, messageSettingsTask.taskUid, { timeout: 600000, interval: 200 });
			// Explicit, deliberate part of a reindex run - ensures the embedder config
			// matches current ACP settings before documents start flowing back in. Gated
			// internally (plugin.updateEmbedders) so it's a no-op unless something about
			// the semantic search config actually changed since it was last applied.
			// Serialized with any concurrent push by plugin.updateEmbedders' mutex, so a
			// save-triggered run re-reads the fingerprint the save path persisted and
			// skips instead of double-pushing.
			await plugin.updateEmbedders(options.settingsData);
			// Federation: remote content lives in sibling zsets (postsRemote:pid /
			// topicsRemote:tid — src/posts/create.js and src/topics/create.js route
			// non-numeric ids there). Totals cover BOTH sets; the loops below
			// iterate the union so remote posts/topics are (re)indexed like local.
			plugin.indexing.topic_progress.total =
				(await db.sortedSetCard('topics:tid') || 0) + (await db.sortedSetCard('topicsRemote:tid') || 0);
			plugin.indexing.post_progress.total =
				(await db.sortedSetCard('posts:pid') || 0) + (await db.sortedSetCard('postsRemote:pid') || 0);
			plugin.indexing.message_progress.total = await db.sortedSetCard('messages:mid') || 0;
			baseBacklog = await snapshotBacklog(plugin);
			emitProgress();
			await Promise.all([
				processSetUnion(
					['topics:tid', 'topicsRemote:tid'],
					async (tids) => {
						plugin.indexing.topic_progress.current += tids.length;
						emitProgress();
						const topics = await Topics.getTopicsFields(tids, ['tid', 'cid', 'uid', 'mainPid', 'title', 'timestamp', 'deleted']);
						const activeTopics = [];
						const deletedTids = [];
						const scheduledTopics = [];
						topics.forEach((topic) => {
							if (!topic || !topic.tid) {
								skippedMissingTopics += 1;
								return;
							}
							if (isFlagSet(topic.deleted)) {
								deletedTids.push(topic.tid);
							} else if (topic.timestamp && topic.timestamp > Date.now()) {
								// Scheduled: live hooks never index these (indexTopic's
								// scheduled branch) — mirror that here, and delete any
								// previously-indexed doc + main post.
								scheduledTopics.push(topic);
							} else {
								activeTopics.push(topic);
							}
						});
						skippedDeletedTopics += deletedTids.length;
						skippedScheduledTopics += scheduledTopics.length;
						if (scheduledTopics.length) {
							await sendDocuments(plugin, 'topic', scheduledTopics.map(t => t.tid), { method: 'delete' });
							const scheduledMainPids = scheduledTopics.map(t => t.mainPid).filter(Boolean);
							if (scheduledMainPids.length) {
								// encoded: remote (URL) main pids live under codec form
								await sendDocuments(plugin, 'post', scheduledMainPids.map(encodePid), { method: 'delete' });
							}
						}
						if (deletedTids.length) {
							await sendDocuments(plugin, 'topic', deletedTids, { method: 'delete' });
						}
						if (!activeTopics.length) return;
						await sendDocuments(plugin, 'topic',
							activeTopics.map(topic => ({
								tid: topic.tid,
								cid: topic.cid,
								uid: topic.uid,
								mainPid: topic.mainPid,
								title: topic.title,
								timestamp: topic.timestamp,
							})), { primaryKey: 'tid' });
					},
				),
				processSetUnion(
					['posts:pid', 'postsRemote:pid'],
					async (pids) => {
						plugin.indexing.post_progress.current += pids.length;
						emitProgress();
						const posts = await Posts.getPostsFields(pids, ['pid', 'tid', 'uid', 'content', 'timestamp', 'deleted']);
						// Fetch topic deleted/scheduled status for posts in this batch
						const tids = [...new Set(posts.map(p => p && p.tid).filter(Boolean))];
						const topicsData = tids.length ? await Topics.getTopicsFields(tids, ['tid', 'deleted', 'timestamp']) : [];
						const tidToDeleted = {};
						const tidToScheduled = {};
						topicsData.forEach((t) => {
							tidToDeleted[t.tid] = isFlagSet(t.deleted);
							tidToScheduled[t.tid] = t.timestamp && t.timestamp > Date.now();
						});

						const activePosts = [];
						const deletedPids = [];
						const scheduledPids = [];
						posts.forEach((post) => {
							if (!post || !post.pid) {
								skippedMissingPosts += 1;
								return;
							}
							if (isFlagSet(post.deleted) || tidToDeleted[post.tid]) {
								deletedPids.push(post.pid);
							} else if (tidToScheduled[post.tid]) {
								// Post of a scheduled topic (at minimum its main post —
								// the only one that exists pre-publish): must not be
								// indexed; remove any previously-indexed doc.
								scheduledPids.push(post.pid);
							} else {
								activePosts.push(post);
							}
						});
						skippedDeletedPosts += deletedPids.length;
						skippedScheduledPosts += scheduledPids.length;
						if (scheduledPids.length) {
							await sendDocuments(plugin, 'post', scheduledPids.map(encodePid), { method: 'delete' });
						}
						if (deletedPids.length) {
							await sendDocuments(plugin, 'post', deletedPids.map(encodePid), { method: 'delete' });
						}
						const activePids = activePosts.map(p => p.pid);
						const cids = activePids.length ? await Posts.getCidsByPids(activePids) : [];
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
						if (!msg || !msg.mid) {
							skippedMissingMessages += 1;
							return;
						}
						if (isFlagSet(msg.deleted)) {
							deletedMids.push(msg.mid);
						} else if (isFlagSet(msg.system)) {
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
						await sendDocuments(plugin, 'chat_message', allDeindexMids, { method: 'delete' });
					}
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
				},
				{ batch: REINDEX_BATCH_SIZE, progress: plugin.indexing.message_progress },
			);
			succeeded = true;
			winston.info('[plugin/meilisearch] Indexing of posts, topics and chat messages complete');
		} catch (err) {
			winston.error(`[plugin/meilisearch] Indexing failed: ${err.message}`);
			errorMsg = err.message;
		} finally {
			// OWNERSHIP GATE: if this run's lock was taken over by a successor (stall
			// exceeded the TTL and another instance reclaimed it via the heartbeat or
			// plain expiry), do NOT drain the shared queue and do NOT publish
			// running=false — both would corrupt the successor's live run (parked ops
			// replayed mid-run; peers flipped to direct writes mid-run). The
			// successor's own finally handles both when it completes; anything this
			// run parked stays queued for it.
			const stillOwnsLock = await isReindexLockOwnedBy(plugin);
			if (!stillOwnsLock) {
				winston.error('[plugin/meilisearch] Reindex lock was lost to another instance mid-run — skipping this run\'s pending-queue drain and completion publish');
				if (plugin.reindexKeepalive) {
					clearInterval(plugin.reindexKeepalive);
					plugin.reindexKeepalive = null;
				}
				plugin.reindexingForced = false;
				plugin.initializingOnAnotherInstance = false;
				// Leave plugin.indexing alone (running=true stays synced — the
				// successor's keepalive/completion owns that state now) and persist
				// this run's outcome with an error note. No drain, no publish, no
				// monitor — releaseReindexLock's owner check makes it a no-op.
				try {
					// F8: JSON round-trip (Redis stringifies nested objects)
					await setJsonSetting(plugin, 'lastReindexResult', {
						success: false,
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
						error: 'reindex lock lost to another instance mid-run (this run\'s remaining batches may interleave with the successor\'s)',
					});
				} catch (err) {
					winston.error(`[plugin/meilisearch] Failed to persist reindex result: ${err.message}`);
				}
				await releaseReindexLock(plugin);
				return;
			}
			// FLAW 1 FIX — drain BEFORE the completion publish. The old order
			// (publish running=false, then drainAll) let every instance resume
			// direct writes while parked ops were still queued: a parked DELETE
			// could replay after a direct re-add of the same document and remove
			// it from the index until the next write. Draining first keeps every
			// write strictly ordered by the queue's FIFO — live writes on all
			// instances keep parking (running=true is still published, kept fresh
			// by the whole-run keepalive which stays live through the drain), and
			// the drain itself runs in the replay context so replayed ops apply
			// directly instead of re-parking (lib/pending-queue.js).
			plugin.reindexingForced = false;
			plugin.initializingOnAnotherInstance = false;
			pubsub.publish('meilisearch:init', false);
			// Captured BEFORE the drain, like the pre-fix sequencing: the run's own
			// enqueue count for the nothing-enqueued meili_progress finalize below.
			const earlyState = plugin.meiliSend;
			let replayedOps = 0;
			let drainErrorMsg = null;
			try {
				// Cluster-safe replay of live writes deferred during this run: every
				// instance's deferrals (this one included) are parked in the shared
				// persistent pending queue. FIFO ordering means everything deferred
				// lands AFTER the reindex batches (fresh content wins). Drained while
				// still holding the reindex lock (and its refresh interval — see
				// releaseReindexLock at the end of the inner finally) so a concurrent
				// run cannot interleave; health ticks backstop anything the time cap
				// or no-progress bail leaves behind. replayedOps feeds the drain
				// verification's B3 downgrade (count mismatches -> inconclusive).
				replayedOps = await drainAll(plugin);
			} catch (err) {
				// The drain failed outright (DB errors): the queue stays as-is for
				// health-tick drains. The completion publish must STILL fire below —
				// otherwise running=true sticks cluster-wide until the staleness
				// self-heal.
				drainErrorMsg = err.message;
				winston.error(`[plugin/meilisearch] Pending-queue drain failed: ${err.message}`);
			} finally {
				// The keepalive and lock refresh run THROUGH the drain (started above,
				// outside this catch) so no instance falsely self-heals mid-drain and
				// the lock cannot expire under a long drain. Stopped here — after the
				// queue is empty (or the drain has bailed) — right before the
				// completion publish.
				if (plugin.reindexKeepalive) {
					clearInterval(plugin.reindexKeepalive);
					plugin.reindexKeepalive = null;
				}
				plugin.indexing.running = false;
				plugin.indexingUpdatedAt = Date.now();
				// Finalize meili_progress for nothing-enqueued SUCCESS runs BEFORE the
				// completion publish, so the single unconditional message below carries
				// the finished state (the ACP banner gate reads meili.drained). Failed
				// runs keep the reset value — the success banner stays suppressed and
				// the failure surfaces via the result card instead.
				if (succeeded && (!earlyState || earlyState.enqueued === 0)) {
					plugin.indexing.meili_progress = { current: 0, total: 0, drained: true };
				}
				// Unconditional completion publish: lodash throttle flush() only fires a
				// PENDING trailing call — when the throttle is idle it no-ops, which left
				// early-failed and empty runs without a running=false broadcast (other
				// instances kept deferring for up to INDEXING_STALE_MS). One idempotent
				// message; a pending trailing flush at most duplicates it.
				pubsub.publish('meilisearch:reindex', plugin.indexing);
				emitProgress.flush();
				await releaseReindexLock(plugin);
			}
			const sendState = plugin.meiliSend;
			const skippedOversized = sendState ? sendState.quarantined : 0;
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
				skippedScheduledTopics,
				skippedScheduledPosts,
				skippedOversized,
				skippedMissingTopics,
				skippedMissingPosts,
				skippedMissingMessages,
				replayedOps,
				error: errorMsg || drainErrorMsg,
			};
			try {
				// F8: JSON round-trip (Redis stringifies nested objects)
				await setJsonSetting(plugin, 'lastReindexResult', lastIndexResult);
				if (succeeded) {
					await settings.set(plugin.id, { indexed: true }, true);
				}
			} catch (err) {
				winston.error(`[plugin/meilisearch] Failed to persist reindex result: ${err.message}`);
			}
			// Start the Meilisearch-side drain monitor (Phase 2): publishes meili_progress
			// (4th ACP progress bar) and, once the task queue drains, verifies the run's
			// outcome server-side (failed tasks + per-index counts) and reports problems.
			// Started last so replayed pending writes count toward the task total, and
			// after the settings.set above so verification's read-modify-write of
			// lastReindexResult merges into the persisted result.
			const quarantinedByIndex = (sendState && sendState.quarantinedByIndex) || {};
			if (sendState && sendState.enqueued > 0) {
				startDrainMonitor(plugin, {
					baseBacklog,
					enqueued: sendState.enqueued,
					firstTaskUid: sendState.firstTaskUid,
					lastTaskUid: sendState.lastTaskUid,
					quarantined: sendState.quarantined,
					replayedOps,
					expected: succeeded ? {
						topic: Math.max(0, (plugin.indexing.topic_progress.total || 0) - skippedDeletedTopics - skippedMissingTopics - skippedScheduledTopics - (quarantinedByIndex.topic || 0)),
						post: Math.max(0, (plugin.indexing.post_progress.total || 0) - skippedDeletedPosts - skippedMissingPosts - skippedScheduledPosts - (quarantinedByIndex.post || 0)),
						chat_message: Math.max(0, (plugin.indexing.message_progress.total || 0) - skippedDeletedMessages - skippedSystemMessages - skippedOrphanMessages - skippedMissingMessages - (quarantinedByIndex.chat_message || 0)),
					} : null,
				});
			}
			// Nothing-enqueued runs finalized meili_progress before the completion publish
			// above (success -> drained:true so the banner fires; failure -> left at the
			// reset value so the success banner stays suppressed).
		}
	}

	plugin.reindex = async function (force = false, options = {}) {
		// Stale-aware: a previous run whose instance died mid-flight leaves running=true
		// with no progress publishes — that must not block new reindexes forever.
		if (plugin.indexing.running && isReindexActive(plugin)) {
			winston.warn('[plugin/meilisearch] Already indexing');
			// Tell admins too: this branch is also where a worker seeded from the lock
			// (mid-run boot, or a stale lock) silently swallows the boot auto-reindex and
			// admin clicks — the lock-refusal alert can never fire because the guard
			// returns before acquireReindexLock. Throttled to one toast per minute by
			// notifyAdmins.
			const stateAgeS = Math.max(1, Math.round((Date.now() - (plugin.indexingUpdatedAt || 0)) / 1000));
			plugin.notifyAdmins('reindex:alreadyRunning', {
				type: 'warning',
				titleKey: '[[meilisearch:admin.reindexAlreadyRunning]]',
				messageKey: 'meilisearch:admin.reindexAlreadyRunningBody',
				messageArgs: [stateAgeS, Math.round(INDEXING_STALE_MS / 60000)],
				message: `Another reindex appears to be active (state age ~${stateAgeS}s). If this is unexpected, the state self-heals ${Math.round(INDEXING_STALE_MS / 60000)} min after it stops updating.`,
			});
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
			meili_progress: {
				current: 0,
				total: null,
				drained: false,
			},
		};
		plugin.indexingUpdatedAt = Date.now();
		// Fresh per-run send state; replace any drain monitor left over from a
		// previous run BEFORE swapping plugin.indexing so the old monitor's tick can
		// never write meili_progress into the new run's object.
		stopDrainMonitor(plugin);
		resetSendState(plugin);
		// No emitProgress here on purpose: running=true is only broadcast after the
		// reindex lock is acquired (in runReindex), so a lock-loser never announces a
		// phantom run cluster-wide. The local state above is enough for this
		// instance's own hooks to defer immediately.
		winston.info(`[plugin/meilisearch] Indexing posts, topics and chat messages${force ? ' (forced)' : ''}`);
		plugin._reindexPromise = runReindex(force, options);
		return plugin._reindexPromise;
	};
};
