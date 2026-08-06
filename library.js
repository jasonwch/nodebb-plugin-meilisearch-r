'use strict';

const winston = nodebb.require('winston');
const _ = nodebb.require('lodash');

const routeHelpers = nodebb.require('./src/routes/helpers');
const settings = nodebb.require('./src/meta/settings');
const Posts = nodebb.require('./src/posts');
const Topics = nodebb.require('./src/topics');
const batch = nodebb.require('./src/batch');
const Sockets = nodebb.require('./src/socket.io');
const pubsub = nodebb.require('./src/pubsub');
const plugins = nodebb.require('./src/plugins');
const db = nodebb.require('./src/database');

const plugin = {};

plugin.id = 'meilisearch';

plugin.healthy = false;

const REINDEX_BATCH_SIZE = 500;
const PENDING_KEY = 'plugin:meilisearch:pending';
const PENDING_MAX = 100000;
const DRAIN_MAX = 500;
const REINDEX_LOCK_KEY = 'plugin:meilisearch:reindex:lock';
const REINDEX_LOCK_TTL = 3600;
const LOCK_REFRESH_INTERVAL = 5 * 60 * 1000;
const REPLAY_OPS = ['indexPost', 'deindexPost', 'indexTopic', 'deindexTopic', 'deindexPostsPurge', 'deindexTopicsPurge', 'reindexTopicPosts', 'deindexTopicPosts', 'restoreTopic', 'changePostOwner', 'changeTopicOwner', 'onTopicMerge', 'onScheduledPublish'];

const emitProgress = _.throttle(() => {
	pubsub.publish('meilisearch:reindex', plugin.indexing);
}, 500, { leading: true, trailing: true });

plugin.indexing = {
	running: false,
	topic_progress: {
		total: null,
		current: null,
	},
	post_progress: {
		total: null,
		current: null,
	},
};

plugin.reindexingForced = false;
plugin.pendingDuringReindex = [];
plugin.reindexLockRefresh = null;

/** @type {import('meilisearch').Meilisearch} */
plugin.client = undefined;
plugin.defaults = {
	host: 'http://localhost:7700',
	apiKey: undefined,
	maxDocuments: undefined,
	indexed: false,
	rankingRules: [
		{ rule: 'words' },
		{ rule: 'typo' },
		{ rule: 'proximity' },
		{ rule: 'attribute' },
		{ rule: 'sort' },
		{ rule: 'exactness' },
	],
	stopWords: [],
	typoTolerance: 'on',
	typoToleranceMinWordSizeOneTypo: 5,
	typoToleranceMinWordSizeTwoTypos: 9,
	typoToleranceDisableOnWords: [],
	synonyms: [],
	healthCheckInterval: 60,
	searchMinTermLength: 2,
	lastReindexResult: {
		success: false,
		finishedAt: null,
		topic_progress: { current: null, total: null },
		post_progress: { current: null, total: null },
		skippedDeletedTopics: 0,
		skippedDeletedPosts: 0,
		error: null,
	},
};

plugin.breakingSettings = [
	'maxDocuments',
	'rankingRules',
	'stopWords',
	'typoTolerance',
	'typoToleranceMinWordSizeOneTypo',
	'typoToleranceMinWordSizeTwoTypos',
	'typoToleranceDisableOnWords',
	'typoToleranceDisableOnAttributes',
	'synonyms',
];
plugin.initialized = false;

plugin.initializingOnAnotherInstance = false;

plugin.healthCheckTask = null;

pubsub.on('meilisearch:reindex', (indexing) => {
	plugin.indexing = indexing;
	Sockets.server.to('admin/plugins/meilisearch').emit('plugins.meilisearch.reindex', plugin.indexing);
});
pubsub.on('meilisearch:init', (initializing) => {
	plugin.initializingOnAnotherInstance = initializing;
});

plugin.init = async function (params) {
	const { router } = params;
	winston.debug('[plugin/meilisearch] Initializing MeiliSearch plugin');
	routeHelpers.setupAdminPageRoute(router, '/admin/plugins/meilisearch', [], async (req, res) => {
		const topicTotal = Number(plugin.indexing.topic_progress.total) || 0;
		const topicCurrent = Number(plugin.indexing.topic_progress.current) || 0;
		const postTotal = Number(plugin.indexing.post_progress.total) || 0;
		const postCurrent = Number(plugin.indexing.post_progress.current) || 0;
		const topicPercent = topicTotal > 0 ? Math.min(100, Math.max(0, Math.round(100 * topicCurrent / topicTotal))) : 0;
		const postPercent = postTotal > 0 ? Math.min(100, Math.max(0, Math.round(100 * postCurrent / postTotal))) : 0;
		const lastReindexResult = await settings.getOne(plugin.id, 'lastReindexResult') || {};
		res.render('admin/plugins/meilisearch', {
			indexing: plugin.indexing,
			topicPercent,
			postPercent,
			lastReindexResult,
		});
	});
	await settings.setOnEmpty(plugin.id, plugin.defaults);
	await plugin.prepareSearch();
	plugin.patchTopicsSearch();
	plugin.initialized = true;
};

// Guard core Topics.search against empty-term throws (core bug: in:topic-<tid> + trailing space yields empty cleanedTerm).
plugin.patchTopicsSearch = function () {
	if (Topics.__meiliOriginalSearch) { return; }
	Topics.__meiliOriginalSearch = Topics.search;
	Topics.search = async function (tid, term) {
		if (!tid || !term || !String(term).trim()) { return []; }
		return Topics.__meiliOriginalSearch.call(Topics, tid, term);
	};
};

plugin.prepareSearch = async function (data, connectionChanged = false) {
	winston.debug(`[plugin/meilisearch] Connecting to MeiliSearch host: ${data?.host || await settings.getOne(plugin.id, 'host')}`);
	const { Meilisearch } = await import('meilisearch');
	plugin.client = new Meilisearch({
		host: data?.host || await settings.getOne(plugin.id, 'host'),
		apiKey: data?.apiKey || await settings.getOne(plugin.id, 'apiKey') || undefined,
	});
	if (plugin.healthCheckTask) clearInterval(plugin.healthCheckTask);
	const intervalRaw = parseInt(data?.healthCheckInterval || await settings.getOne(plugin.id, 'healthCheckInterval'), 10);
	const intervalSeconds = Number.isFinite(intervalRaw) && intervalRaw > 0
		? Math.min(Math.max(intervalRaw, 10), 3600)
		: 60;
	plugin.healthCheckTask = setInterval(plugin.checkHealth, intervalSeconds * 1000);
	// #14: Don't auto-reindex after a failed reindex unless connection settings changed.
	const indexed = await settings.getOne(plugin.id, 'indexed');
	if (indexed) return;
	const healthy = await plugin.checkHealth();
	if (!healthy || plugin.initializingOnAnotherInstance) return;
	const lastResult = await settings.getOne(plugin.id, 'lastReindexResult') || {};
	const allowAutoReindex = connectionChanged || lastResult.success !== false;
	if (!allowAutoReindex) {
		winston.warn('[plugin/meilisearch] Skipping auto-reindex: last reindex failed. Trigger manually from ACP.');
		return;
	}
	await plugin.updateIndexSettings();
	await plugin.reindex(false);
};

plugin.addRoutes = async function ({ router, middleware, helpers }) {
	const middlewares = [
		middleware.ensureLoggedIn,
		middleware.admin.checkPrivileges,
	];
	routeHelpers.setupApiRoute(router, 'get', '/meilisearch/reindex', middlewares, async (req, res) => {
		helpers.formatApiResponse(200, res, {
			indexing: plugin.indexing,
			healthy: plugin.healthy,
			lastReindexResult: await settings.getOne(plugin.id, 'lastReindexResult') || {},
		});
	});

	routeHelpers.setupApiRoute(router, 'post', '/meilisearch/reindex', middlewares, async (req, res) => {
		await settings.set(plugin.id, { indexed: false }, true);
		plugin.reindex(false);
		helpers.formatApiResponse(202, res, plugin.indexing);
	});
	routeHelpers.setupApiRoute(router, 'delete', '/meilisearch/reindex', middlewares, async (req, res) => {
		await settings.set(plugin.id, { indexed: false }, true);
		plugin.reindex(true);
		helpers.formatApiResponse(202, res, plugin.indexing);
	});
};

plugin.addAdminNavigation = function (header) {
	header.plugins.push({
		route: '/plugins/meilisearch',
		icon: 'fa-tint',
		name: 'Meilisearch',
	});

	return header;
};

plugin.checkHealth = async function () {
	try {
		plugin.healthy = await plugin.client.isHealthy();
		if (!plugin.healthy) {
			winston.warn('[plugin/meilisearch] MeiliSearch host is unhealthy');
		} else {
			drainPending().catch(err => winston.error(`[plugin/meilisearch] drain failed: ${err.message}`));
		}
		return plugin.healthy;
	} catch (err) {
		plugin.healthy = false;
		winston.warn(`[plugin/meilisearch] ${err.message}`);
		return false;
	}
};

async function maybeDefer(op, payload) {
	if (plugin.reindexingForced) {
		if (plugin.pendingDuringReindex.length >= PENDING_MAX) {
			plugin.pendingDuringReindex.shift();
		}
		plugin.pendingDuringReindex.push({ op, payload });
		return true;
	}
	if (!plugin.healthy && !(await plugin.checkHealth())) {
		await enqueuePending(op, payload);
		return true;
	}
	return false;
}

async function enqueuePending(op, payload) {
	const count = await db.sortedSetCard(PENDING_KEY);
	if (count >= PENDING_MAX) {
		const oldest = await db.getSortedSetRange(PENDING_KEY, 0, 0);
		if (oldest.length) await db.sortedSetRemove(PENDING_KEY, ...oldest);
	}
	await db.sortedSetAdd(PENDING_KEY, Date.now(), JSON.stringify({ op, payload }));
}

async function meiliWrite(op, payload, fn) {
	try {
		await fn();
	} catch (err) {
		winston.error(`[plugin/meilisearch] ${op} write failed: ${err.message}`);
		plugin.healthy = false;
		await enqueuePending(op, payload);
	}
}

async function drainPending() {
	if (!plugin.healthy || plugin.reindexingForced) return;
	let drained = 0;
	for (;;) {
		if (!plugin.healthy) break;
		const members = await db.getSortedSetRange(PENDING_KEY, 0, 49);
		if (!members.length) break;
		await db.sortedSetRemove(PENDING_KEY, ...members);
		for (const member of members) {
			try {
				const { op, payload } = JSON.parse(member);
				if (op && REPLAY_OPS.includes(op)) {
					await plugin[op](payload);
				}
			} catch (err) {
				winston.error(`[plugin/meilisearch] replay failed: ${err.message}`);
			}
			drained += 1;
		}
		if (drained >= DRAIN_MAX) break;
	}
}

plugin.getNotices = async function (notices) {
	const checkHealth = await plugin.checkHealth();
	notices.push({
		done: checkHealth,
		doneText: 'MeiliSearch connection OK',
		notDoneText: 'Could not connect to MeiliSearch',
	});
	return notices;
};

plugin.updateIndexSettings = async (data) => {
	await ensureIndex('post', 'pid');
	await ensureIndex('topic', 'tid');
	data = {
		maxDocuments: parseInt(data?.maxDocuments || await settings.getOne(plugin.id, 'maxDocuments') || 500, 10),
		rankingRules: (data?.rankingRules || await settings.getOne(plugin.id, 'rankingRules'))?.map(value => value.rule),
		stopWords: (data?.stopWords || await settings.getOne(plugin.id, 'stopWords'))?.map(value => value.word),
		typoTolerance: ['on', true].includes(
			data?.typoTolerance || await settings.getOne(plugin.id, 'typoTolerance') || undefined,
		),
		typoToleranceMinWordSizeOneTypo: parseInt(
			data?.typoToleranceMinWordSizeOneTypo ||
				await settings.getOne(plugin.id, 'typoToleranceMinWordSizeOneTypo') || 5,
			10,
		),
		typoToleranceMinWordSizeTwoTypos: parseInt(
			data?.typoToleranceMinWordSizeTwoTypos ||
				await settings.getOne(plugin.id, 'typoToleranceMinWordSizeTwoTypos') || 9,
			10,
		),
		typoToleranceDisableOnWords:
			(data?.typoToleranceDisableOnWords || await settings.getOne(plugin.id, 'typoToleranceDisableOnWords') ||
				undefined)?.map(value => value.word),
		synonyms: Object.fromEntries(
			(data?.synonyms || await settings.getOne(plugin.id, 'synonyms') || [])?.map((
				{ word, synonyms },
			) => [word, synonyms?.split(',').map(synonym => synonym.trim())]),
		),
	};
	const postTask = await plugin.client.index('post').updateSettings({
		filterableAttributes: ['tid', 'cid', 'uid', 'timestamp'],
		sortableAttributes: ['timestamp', 'cid'],
		searchableAttributes: ['content'],
		pagination: {
			maxTotalHits: data.maxDocuments,
		},
		rankingRules: data.rankingRules,
		stopWords: data.stopWords,
		typoTolerance: {
			enabled: data.typoTolerance,
			minWordSizeForTypos: {
				oneTypo: data.typoToleranceMinWordSizeOneTypo,
				twoTypos: data.typoToleranceMinWordSizeTwoTypos,
			},
			disableOnWords: data.typoToleranceDisableOnWords,
		},
		synonyms: data.synonyms,
	});
	const topicTask = await plugin.client.index('topic').updateSettings({
		filterableAttributes: ['cid', 'uid', 'timestamp'],
		sortableAttributes: ['cid', 'title', 'timestamp'],
		searchableAttributes: ['title'],
		pagination: {
			maxTotalHits: data.maxDocuments,
		},
		rankingRules: data.rankingRules,
		stopWords: data.stopWords,
		typoTolerance: {
			enabled: data.typoTolerance,
			minWordSizeForTypos: {
				oneTypo: data.typoToleranceMinWordSizeOneTypo,
				twoTypos: data.typoToleranceMinWordSizeTwoTypos,
			},
			disableOnWords: data.typoToleranceDisableOnWords,
		},
		synonyms: data.synonyms,
	});
	return [postTask, topicTask];
};

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
	};
	emitProgress();
	winston.info(`[plugin/meilisearch] Indexing posts and topics${force ? ' (forced)' : ''}`);
	plugin._reindexPromise = runReindex(force);
	return plugin._reindexPromise;
};

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
			await plugin.client.tasks.waitForTask(postTask.taskUid);
			await plugin.client.tasks.waitForTask(topicTask.taskUid);
		}
		const [postSettingsTask, topicSettingsTask] = await plugin.updateIndexSettings();
		await plugin.client.tasks.waitForTask(postSettingsTask.taskUid);
		await plugin.client.tasks.waitForTask(topicSettingsTask.taskUid);
		plugin.indexing.topic_progress.total = await db.sortedSetCard('topics:tid') || 0;
		plugin.indexing.post_progress.total = await db.sortedSetCard('posts:pid') || 0;
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
		succeeded = true;
		winston.info('[plugin/meilisearch] Indexing complete');
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
			skippedDeletedTopics,
			skippedDeletedPosts,
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

plugin.indexPost = async function ({ post, tid: newTid }) {
	const deferPayload = { post, tid: newTid };
	if (await maybeDefer('indexPost', deferPayload)) return;
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
	await meiliWrite('indexPost', { post: { pid: post.pid } }, () =>
		plugin.client.index('post').updateDocuments([doc], { primaryKey: 'pid' }));
};

plugin.deindexPost = async function ({ post }) {
	if (await maybeDefer('deindexPost', { post })) return;
	await meiliWrite('deindexPost', { post }, () =>
		plugin.client.index('post').deleteDocument(post.pid));
};

plugin.indexTopic = async function ({ topic, tid, fromCid, post }) {
	const isMove = fromCid !== undefined;
	if (await maybeDefer('indexTopic', { topic, tid, fromCid, post })) return;
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
	await meiliWrite('indexTopic', { topic, tid, fromCid, post }, () =>
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
		winston.error(`[plugin/meilisearch] reindexTopicPosts failed: ${err.message}`);
		plugin.healthy = false;
		await enqueuePending('reindexTopicPosts', { tid });
	}
};

plugin.deindexTopic = async function ({ topic, tid }) {
	if (await maybeDefer('deindexTopic', { topic, tid })) return;
	// Tolerate topic.move-style payloads (no 'topic' key) — extract tid.
	const topicTid = topic ? topic.tid : tid;
	if (!topicTid) {
		winston.warn('[plugin/meilisearch] deindexTopic: no tid');
		return;
	}
	await meiliWrite('deindexTopic', { topic, tid }, () =>
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
		winston.error(`[plugin/meilisearch] deindexTopicPosts failed: ${err.message}`);
		plugin.healthy = false;
		await enqueuePending('deindexTopicPosts', { tid });
	}
};

plugin.restoreTopic = async function ({ topic, tid }) {
	const topicTid = topic ? topic.tid : tid;
	if (await maybeDefer('restoreTopic', { topic, tid })) return;
	// Re-fetch topic if missing (defensive — same pattern as indexTopic).
	if (!topic && topicTid) {
		topic = await Topics.getTopicData(topicTid);
	}
	if (!topic) {
		winston.warn('[plugin/meilisearch] restoreTopic: no topic data');
		return;
	}
	// Re-index the topic document.
	await meiliWrite('restoreTopic', { topic, tid }, () =>
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

plugin.changePostOwner = async function ({ posts, toUid }) {
	if (await maybeDefer('changePostOwner', { posts, toUid })) return;
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
	await meiliWrite('changePostOwner', { posts: activePids.map(pid => ({ pid })), toUid }, () =>
		plugin.client.index('post').updateDocuments(docs, { primaryKey: 'pid' }));
};

plugin.changeTopicOwner = async function ({ topics, toUid }) {
	if (await maybeDefer('changeTopicOwner', { topics, toUid })) return;
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
	await meiliWrite('changeTopicOwner', { topics: activeTopics.map(t => ({ tid: t.tid })), toUid }, () =>
		plugin.client.index('topic').updateDocuments(docs, { primaryKey: 'tid' }));
};

plugin.onTopicMerge = async function ({ otherTids, mergeIntoTid }) {
	if (await maybeDefer('onTopicMerge', { otherTids, mergeIntoTid })) return;
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
				await meiliWrite('deindexPostsPurge', { posts: orphanPids.map(pid => ({ pid })) }, () =>
					postIndex.deleteDocuments(orphanPids));
			}
		} catch (err) {
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
	if (await maybeDefer('onScheduledPublish', { topics: tids.map(tid => ({ tid })) })) return;
	// Re-index each published topic + its posts (DB deleted=0, timestamp in past now).
	for (const tid of tids) {
		await plugin.indexTopic({ tid });
		if (plugin.healthy) {
			plugin.reindexTopicPosts({ tid }).catch(err =>
				winston.error(`[plugin/meilisearch] onScheduledPublish reindexTopicPosts failed: ${err.message}`));
		}
	}
};

plugin.deindexPostsPurge = async function ({ posts }) {
	if (await maybeDefer('deindexPostsPurge', { posts })) return;
	const pids = Array.isArray(posts) ? posts.map(p => p && p.pid).filter(Boolean) : [];
	if (!pids.length) {
		return;
	}
	const reducedPayload = { posts: pids.map(pid => ({ pid })) };
	await meiliWrite('deindexPostsPurge', reducedPayload, () =>
		plugin.client.index('post').deleteDocuments(pids));
};

plugin.deindexTopicsPurge = async function ({ topics }) {
	if (await maybeDefer('deindexTopicsPurge', { topics })) return;
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
		winston.error(`[plugin/meilisearch] deindexTopicsPurge failed: ${err.message}`);
		plugin.healthy = false;
		await enqueuePending('deindexTopicsPurge', reducedPayload);
	}
};

plugin.checkConflict = function () {
	const hooksToCheck = [
		'filter:search.query',
		'filter:topic.search',
	];
	// blacklist, in case someone makes a plugin using these hooks that doesn't conflict.
	// also, outside of dbsearch the user is expected to realize they installed two search plugins.
	const conflictingPlugins = [
		'nodebb-plugin-dbsearch',
		'nodebb-plugin-solr',
		'nodebb-plugin-elasticsearch',
		'nodebb-plugin-search-elasticsearch',
	];
	for (const hook of hooksToCheck) {
		if ((plugins.loadedHooks[hook] || []).filter(hookData => conflictingPlugins.includes(hookData.id)).length >= 1) {
			return true;
		}
	}
	return false;
};

plugin.search = async function (data) {
	if (plugin.checkConflict()) {
		// The dbsearch plugin was detected, abort search!
		winston.warn(
			'[plugin/meilisearch] Another search plugin (most likely dbsearch) is enabled, so search via Meilisearch was aborted.',
		);
		return data;
	}
	if (!plugin.healthy && !(await plugin.checkHealth())) {
		winston.warn(
			'[plugin/meilisearch] Meilisearch instance did not return a healthy response, so search via Meilisearch was aborted.',
		);
		return data;
	}
	const rawQuery = (data.term || data.content || '').trim();
	const minTermLength = await resolveMinTermLength();
	if (!rawQuery || !rawQuery.split(' ').some(word => word.length >= minTermLength)) {
		winston.debug(`[plugin/meilisearch] Skipping search: no query word >= ${minTermLength} char(s)`);
		return data;
	}
	// #6: Use local variables instead of mutating the shared payload.
	const content = data.term || data.content;
	const searchData = data.term ? { tid: data.tid } : data?.searchData;
	const rawIndex = Array.isArray(data?.index) ? data.index[0] : data.index;
	const ALLOWED_INDEXES = { post: 'pid', topic: 'tid' };
	const index = ALLOWED_INDEXES[rawIndex] ? rawIndex : 'post';
	const id = ALLOWED_INDEXES[index];
	winston.debug(`[plugin/meilisearch] Searching for ${content} in ${index}`);
	const result = await plugin.client.index(index).search(content, {
		attributesToRetrieve: [id],
		limit: parseInt(await settings.getOne(plugin.id, 'maxDocuments') || 500, 10),
		filter: plugin.buildFilter(
			data.cid,
			data.uid,
			searchData?.timeFilter,
			searchData?.timeRange,
			searchData?.tid,
		),
		sort: plugin.buildSort(searchData?.sortBy, searchData?.sortDirection),
		matchingStrategy: data.matchWords === 'all' ? 'all' : 'last',
	});
	data.ids = result.hits.map(hit => hit[id]);
	return data;
};

plugin.buildFilter = function (categories, postedBy, timeFilter, timeRange, tid) {
	const num = (v) => {
		const n = parseInt(v, 10);
		return Number.isFinite(n) ? n : null;
	};
	const filter = [];
	if (categories?.length) {
		const cids = categories.map(num).filter(c => c !== null);
		if (cids.length) filter.push(cids.map(cid => `cid = ${cid}`));
	}
	if (postedBy?.length) {
		const uids = postedBy.map(num).filter(u => u !== null);
		if (uids.length) filter.push(uids.map(uid => `uid = ${uid}`));
	}
	if (timeFilter && timeRange) {
		const range = num(timeRange);
		if (range !== null) {
			filter.push(`timestamp ${timeFilter === 'newer' ? '>' : '<'} ${Date.now() - (range * 1000)}`);
		}
	}
	const numericTid = num(tid);
	if (numericTid !== null) {
		filter.push(`tid = ${numericTid}`);
	}
	return filter.length ? filter : undefined;
};

plugin.buildSort = function (sortBy, sortDirection) {
	let field = '';
	switch (sortBy) {
		case 'timestamp':
			field = 'timestamp';
			break;
		case 'topic.title':
			field = 'title';
			break;
		case 'category':
			field = 'cid';
			break;
		default:
			return undefined;
	}
	return [`${field}:${sortDirection === 'ascending' ? 'asc' : 'desc'}`];
};

plugin.saveSettings = async (data) => {
	if (data.plugin === plugin.id && !data.quiet && plugin.initialized) {
		try {
			if (data.settings && Object.prototype.hasOwnProperty.call(data.settings, 'searchMinTermLength')) {
				data.settings.searchMinTermLength = clampMinTermLength(data.settings.searchMinTermLength);
			}
			// #8: Only re-connect when connection settings changed.
			const connChanged = await connectionSettingsChanged(data.settings);
			if (connChanged) {
				// Fix 10: If host changed, clear indexed so prepareSearch auto-reindexes the new (empty) host.
				const hostChanged = await hostSettingChanged(data.settings);
				if (hostChanged) {
					await settings.set(plugin.id, { indexed: false }, true);
				}
				await plugin.prepareSearch(data.settings, true);
			}
			const changed = (await Promise.all(
				Object.entries(data.settings).map(([k, v]) => isBreaking([k, v])),
			)).some(Boolean);
			if (changed) {
				winston.info('settings changed, updating index');
				await plugin.updateIndexSettings(data.settings);
			}
		} catch (err) {
			// #7: Don't throw — let settings persist to DB even when Meili is unreachable.
			winston.error(`[plugin/meilisearch] Error while saving settings: ${err.message}`);
			plugin.healthy = false;
		}
	}
	return data;
};

async function ensureIndex(uid, primaryKey) {
	try {
		await plugin.client.getIndex(uid);
	} catch (e) {
		await plugin.client.createIndex(uid, { primaryKey });
	}
}

async function connectionSettingsChanged(newSettings) {
	if (!newSettings) return false;
	const keys = ['host', 'apiKey', 'healthCheckInterval'];
	for (const k of keys) {
		if (!Object.prototype.hasOwnProperty.call(newSettings, k)) continue;
		const stored = await settings.getOne(plugin.id, k);
		if (String(stored || '') !== String(newSettings[k] || '')) return true;
	}
	return false;
}

async function hostSettingChanged(newSettings) {
	if (!newSettings || !Object.prototype.hasOwnProperty.call(newSettings, 'host')) return false;
	const stored = await settings.getOne(plugin.id, 'host');
	return String(stored || '') !== String(newSettings.host || '');
}

async function isBreaking([setting, value]) {
	if (!plugin.breakingSettings.includes(setting)) {
		return false;
	}
	const stored = await settings.getOne(plugin.id, setting);
	const changed = !deepCompare(stored, value);
	if (changed) {
		winston.info(`[plugin/meilisearch] ${setting} changed: ${JSON.stringify(stored)} -> ${JSON.stringify(value)}`);
	}
	return changed;
}

async function resolveMinTermLength() {
	const v = parseInt(await settings.getOne(plugin.id, 'searchMinTermLength'), 10);
	return Number.isFinite(v) && v >= 2 ? v : 2;
}

function clampMinTermLength(value) {
	const v = parseInt(value, 10);
	return Number.isFinite(v) && v >= 2 ? v : 2;
}

function deepCompare(a, b) {
	if (a === null || b === null) return a === b;
	if (typeof a !== typeof b) return false;
	switch (typeof a) {
		case 'object':
			return Object.keys(a).length === Object.keys(b).length &&
				Object.keys(a).every(key => deepCompare(a[key], b[key]));
		case 'string':
		case 'number':
		case 'boolean':
		default:
			return a === b;
	}
}
module.exports = plugin;
