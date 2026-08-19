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
	// Sync wait window for plugin.updateEmbedders' Meilisearch task before falling back to
	// background poller. 60s keeps ACP save responsive; if the embedder task is still
	// running server-side (re-embedding every doc legitimately exceeds 60s on a forum of
	// any size), updateEmbedders persists the fingerprint optimistically and startEmbedderPoller
	// verifies the eventual outcome.
	EMBEDDER_TASK_TIMEOUT_MS: 60000,
	// Background poller ceiling (60 min) and interval (5 s) for verifying embedder tasks
	// that exceeded the sync wait window. 60 min covers re-embedding up to ~1M posts at
	// OpenAI Tier 1 rate limits (1M TPM). For forums larger than that, the poller gives up
	// and relies on reattachPollers to reconcile the task outcome on the next restart
	// (within Meilisearch's ~24h task retention window). No double-paid-API-cost risk —
	// fingerprint is preserved, not cleared, on poller timeout (see startEmbedderPoller
	// MeilisearchTaskTimeOutError handling in lib/client.js).
	EMBEDDER_TASK_BG_TIMEOUT_MS: 3600000,
	EMBEDDER_TASK_BG_INTERVAL_MS: 5000,
	// Meilisearch SDK client-side HTTP timeout. Caps every fetch() the SDK makes (including
	// the initial enqueue POST for embedder tasks) at 30s — without this, the SDK uses no
	// AbortSignal (node_modules/meilisearch/dist/index.js:340), so an unreachable Meilisearch
	// host can leave plugin.updateEmbedders (and therefore the ACP Save button) stuck
	// indefinitely.
	MEILI_HTTP_TIMEOUT_MS: 30000,
};
