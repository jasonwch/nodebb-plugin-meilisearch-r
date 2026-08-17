'use strict';

const settings = nodebb.require('./src/meta/settings');

// Fixed embedder name used across all three indexes. Meilisearch supports multiple
// named embedders per index, but this plugin only ever needs one at a time.
const EMBEDDER_NAME = 'default';

// ACP field names that feed buildEmbedderConfig(). Kept as a single list so callers
// (lib/settings.js) can tell "an embedder-relevant field changed" apart from any other
// breaking setting (ranking rules, stop words, ...) - re-pushing embedders unnecessarily
// risks Meilisearch re-embedding every document through a paid API for no reason.
const SEMANTIC_SETTING_KEYS = [
	'semanticSearchEnabled',
	'semanticSearchProvider',
	'semanticSearchApiKey',
	'semanticSearchModel',
	'semanticSearchUrl',
	'semanticSearchDimensions',
	'semanticSearchRestRequest',
	'semanticSearchRestResponse',
];

// documentTemplate is index-specific (each index has different fields) so it isn't
// exposed in the ACP - only the provider/credentials/ratio are admin-configurable.
const DOCUMENT_TEMPLATES = {
	post: 'A forum post: {{doc.content}}',
	topic: 'A forum topic titled: {{doc.title}}',
	chat_message: 'A chat message: {{doc.content}}',
};

function truthy(value) {
	return ['on', true, 'true'].includes(value);
}

async function get(plugin, data, key, fallback) {
	if (data && Object.prototype.hasOwnProperty.call(data, key)) {
		return data[key] === undefined || data[key] === '' ? fallback : data[key];
	}
	const stored = await settings.getOne(plugin.id, key);
	return stored === undefined || stored === '' ? fallback : stored;
}

async function isSemanticSearchEnabled(plugin, data) {
	return truthy(await get(plugin, data, 'semanticSearchEnabled', false));
}

async function getSemanticRatio(plugin, data) {
	const raw = parseFloat(await get(plugin, data, 'semanticSearchRatio', 0.5));
	return Number.isFinite(raw) ? Math.min(Math.max(raw, 0), 1) : 0.5;
}

// Meilisearch's rankingScoreThreshold defaults to 0 (no filtering) when omitted, meaning
// vector search always returns its `limit` nearest neighbours even when none of them are
// actually a good match for the query - there's no built-in "too dissimilar, drop it" cutoff.
// This is what surfaces as "every query returns exactly N results" (N = total documents in
// the index) with garbage in the tail. Defaulting to a modest 0.2 here (rather than 0, which
// would silently restore the unfiltered behavior) keeps genuinely unrelated hits out while
// still letting fuzzy-but-real semantic matches through; admins can tune it in the ACP.
async function getSemanticScoreThreshold(plugin, data) {
	const raw = parseFloat(await get(plugin, data, 'semanticSearchScoreThreshold', 0.2));
	return Number.isFinite(raw) ? Math.min(Math.max(raw, 0), 1) : 0.2;
}

function parseJsonTemplate(raw) {
	if (!raw || typeof raw !== 'string' || !raw.trim()) return undefined;
	try {
		return JSON.parse(raw);
	} catch (e) {
		return undefined;
	}
}

// Builds the embedder config object for one index, or null to remove the embedder
// (Meilisearch deletes an embedder when its value is set to null in updateEmbedders).
async function buildEmbedderConfig(plugin, data, indexName) {
	if (!(await isSemanticSearchEnabled(plugin, data))) {
		return null;
	}
	const provider = await get(plugin, data, 'semanticSearchProvider', 'openAi');
	const dimensions = parseInt(await get(plugin, data, 'semanticSearchDimensions', ''), 10);
	const base = {
		source: provider,
		documentTemplate: DOCUMENT_TEMPLATES[indexName],
	};
	if (Number.isFinite(dimensions) && dimensions > 0) {
		base.dimensions = dimensions;
	}
	switch (provider) {
		case 'openAi': {
			const apiKey = await get(plugin, data, 'semanticSearchApiKey', undefined);
			const model = await get(plugin, data, 'semanticSearchModel', 'text-embedding-3-small');
			// Optional: overrides the default api.openai.com endpoint so OpenAI-compatible
			// providers (OpenRouter, Azure OpenAI, LocalAI, ...) can be used via the "openAi"
			// source directly, without needing the fully custom "rest" request/response templates.
			const url = await get(plugin, data, 'semanticSearchUrl', undefined);
			return { ...base, apiKey, model, ...(url ? { url } : {}) };
		}
		case 'huggingFace': {
			const model = await get(plugin, data, 'semanticSearchModel', 'BAAI/bge-base-en-v1.5');
			return { ...base, model };
		}
		case 'ollama': {
			const url = await get(plugin, data, 'semanticSearchUrl', 'http://localhost:11434/api/embeddings');
			const model = await get(plugin, data, 'semanticSearchModel', 'nomic-embed-text');
			const apiKey = await get(plugin, data, 'semanticSearchApiKey', undefined);
			return { ...base, url, model, apiKey };
		}
		case 'rest': {
			const url = await get(plugin, data, 'semanticSearchUrl', undefined);
			const apiKey = await get(plugin, data, 'semanticSearchApiKey', undefined);
			// "{{..}}" as the 2nd array element (in BOTH templates) tells Meilisearch this
			// endpoint accepts/returns a batch of texts, not just one - Meilisearch batches
			// multiple documents' texts into a single HTTP call during indexing for throughput.
			// Without it here, a batched request (multiple texts) gets a response template that
			// can only decode one embedding, failing with "response has a single embedding, but
			// request has multiple texts to embed".
			const request = parseJsonTemplate(await get(plugin, data, 'semanticSearchRestRequest', undefined)) ||
				{ input: ['{{text}}', '{{..}}'], model: 'text-embedding-3-small' };
			const response = parseJsonTemplate(await get(plugin, data, 'semanticSearchRestResponse', undefined)) ||
				{ data: [{ embedding: '{{embedding}}' }, '{{..}}'] };
			return {
				...base, url, apiKey, request, response,
			};
		}
		default:
			return null;
	}
}

function deepEqual(a, b) {
	if (a === b) return true;
	if (typeof a !== typeof b || a === null || b === null) return false;
	if (typeof a !== 'object') return false;
	const aKeys = Object.keys(a);
	const bKeys = Object.keys(b);
	if (aKeys.length !== bKeys.length) return false;
	return aKeys.every(key => deepEqual(a[key], b[key]));
}

// Per-index bookkeeping of the last embedder config actually pushed to Meilisearch,
// so buildEmbedderConfig() can be re-run freely (e.g. as a side effect of unrelated
// settings saves) without re-issuing an identical updateEmbedders() call - Meilisearch
// treats every updateEmbedders() call as "this embedder's config is new", which is the
// one operation that can re-trigger paid-API re-embedding of every document.
async function getAppliedConfig(plugin, indexName) {
	const stored = await settings.getOne(plugin.id, 'appliedEmbedders') || {};
	return Object.prototype.hasOwnProperty.call(stored, indexName) ? stored[indexName] : undefined;
}

// Persists multiple indexes' applied configs in a single read-modify-write, so concurrent
// per-index updates can't clobber each other's bookkeeping (see lib/client.js updateEmbedders).
async function setAppliedConfigs(plugin, plans) {
	const stored = await settings.getOne(plugin.id, 'appliedEmbedders') || {};
	plans.forEach(({ indexName, config }) => {
		stored[indexName] = config;
	});
	await settings.set(plugin.id, { appliedEmbedders: stored }, true);
}

// Clears the "last applied" bookkeeping - call this whenever the Meilisearch connection
// itself changes (new host). A fresh host has no memory of what this plugin last pushed,
// so the next updateEmbedders() call must be allowed through even if the config value is
// identical to what was applied to the OLD host.
async function resetAppliedConfigs(plugin) {
	await settings.set(plugin.id, { appliedEmbedders: {} }, true);
}

module.exports = {
	EMBEDDER_NAME,
	SEMANTIC_SETTING_KEYS,
	isSemanticSearchEnabled,
	getSemanticRatio,
	getSemanticScoreThreshold,
	buildEmbedderConfig,
	deepEqual,
	getAppliedConfig,
	setAppliedConfigs,
	resetAppliedConfigs,
};
