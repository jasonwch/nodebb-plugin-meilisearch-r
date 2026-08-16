'use strict';

module.exports = {
	REINDEX_BATCH_SIZE: 500,
	PENDING_KEY: 'plugin:meilisearch:pending',
	PENDING_MAX: 100000,
	DRAIN_MAX: 500,
	REINDEX_LOCK_KEY: 'plugin:meilisearch:reindex:lock',
	REINDEX_LOCK_TTL: 3600,
	LOCK_REFRESH_INTERVAL: 5 * 60 * 1000,
	REPLAY_OPS: [
		'indexPost', 'deindexPost', 'indexTopic', 'deindexTopic', 'deindexPostsPurge',
		'deindexTopicsPurge', 'reindexTopicPosts', 'deindexTopicPosts', 'restoreTopic',
		'changePostOwner', 'changeTopicOwner', 'onTopicMerge', 'onScheduledPublish',
		'indexMessage', 'deindexMessage',
	],
	GLOBAL_CHAT_SEARCH_LIMIT: 200,
	GLOBAL_CHAT_SEARCH_FETCH: 300,
	MEMBERSHIP_CHUNK_SIZE: 100,
	DECORATION_BATCH_SIZE: 10,
};
