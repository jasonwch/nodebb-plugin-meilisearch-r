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

// Reads one config value from the in-progress save payload (data) or the persisted settings.
// Always .trim()s string values so padded input (e.g. " sk-abc " pasted by admin) doesn't reach
// the embedder config → embedding API (OpenAI returns 401 for keys with trailing whitespace).
// Client-side .trim() in admin.js validateEmbedderForm is the primary UX guard, but settings.save
// serializes form values via jQuery .val() which returns raw (untrimmed) strings; this server-side
// trim is defense-in-depth for settings-save AND for reindex/prepareSearch paths that read from DB.
async function get(plugin, data, key, fallback) {
	let value;
	if (data && Object.prototype.hasOwnProperty.call(data, key)) {
		value = data[key];
	} else {
		value = await settings.getOne(plugin.id, key);
	}
	if (value === undefined || value === '') return fallback;
	if (typeof value === 'string') value = value.trim();
	return value === '' ? fallback : value;
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

// Pre-flight validation: per-provider check of required fields BEFORE attempting to push
// the embedder config to Meilisearch. Returns null if config is valid (or semantic search
// is disabled — nothing to validate), or an error string listing the missing/invalid fields.
//
// Mirrors buildEmbedderConfig's per-provider defaults: a field is flagged "missing" only
// when it has no default fallback AND no edge-case override applies. Specifically:
// - openAi: apiKey required ONLY when url is blank (custom OpenAI-compatible endpoints
//   like LocalAI dev mode may not require auth). model/url-based defaults cover the rest.
// - huggingFace: nothing required (HF runs inside Meilisearch; model defaulted).
// - ollama: nothing required (model + url defaulted for local install).
// - rest: url required (no default); request/response templates have defaults if missing
//   but warn if non-empty invalid JSON (admin's template dropped silently otherwise).
//
// Used by lib/client.js plugin.updateEmbedders (defense-in-depth, server-side). A parallel
// client-side validator in static/lib/admin.js mirrors this logic to block the ACP Save
// before any POST — primary UX path.
async function validateEmbedderConfig(plugin, data) {
	if (!(await isSemanticSearchEnabled(plugin, data))) return null;
	const provider = await get(plugin, data, 'semanticSearchProvider', 'openAi');
	const missing = [];
	const invalid = [];
	switch (provider) {
		case 'openAi': {
			const apiKey = await get(plugin, data, 'semanticSearchApiKey', undefined);
			const url = await get(plugin, data, 'semanticSearchUrl', undefined);
			// model has a default; URL is optional. apiKey is required ONLY when URL is blank
			// (admin is using OpenAI's default api.openai.com endpoint, which requires auth).
			// If URL is set, admin is pointing at a custom OpenAI-compatible endpoint that may
			// not require auth — don't block legitimate no-auth setups.
			if (!apiKey && !url) missing.push('apiKey');
			break;
		}
		case 'huggingFace':
			// Model has a default; HF runs in Meilisearch itself - nothing strictly required.
			break;
		case 'ollama':
			// url + model have defaults; apiKey optional. Nothing strictly required for local install.
			break;
		case 'rest': {
			const url = await get(plugin, data, 'semanticSearchUrl', undefined);
			if (!url) missing.push('url');
			// request/response have defaults if missing/invalid. But a non-empty non-JSON string
			// would silently fall back to default via parseJsonTemplate — admin's template dropped
			// without warning. Warn (don't block) on that here too.
			const request = await get(plugin, data, 'semanticSearchRestRequest', undefined);
			const response = await get(plugin, data, 'semanticSearchRestResponse', undefined);
			if (request && request.trim() && parseJsonTemplate(request) === undefined) {
				invalid.push('request');
			}
			if (response && response.trim() && parseJsonTemplate(response) === undefined) {
				invalid.push('response');
			}
			break;
		}
		default:
			return `Unknown provider: ${provider}`;
	}
	if (!missing.length && !invalid.length) return null;
	const parts = [];
	if (missing.length) parts.push(`Required field(s) missing for "${provider}" provider: ${missing.join(', ')}`);
	if (invalid.length) parts.push(`Invalid JSON in field(s): ${invalid.join(', ')}`);
	return parts.join('; ');
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

// Clears one index's "last applied" bookkeeping - used by the embedder background poller
// in lib/client.js when a delayed task failure is detected: the fingerprint was
// optimistically persisted at enqueue time, so we must clear it on eventual failure to
// force the next save to push fresh config (rather than silently skipping as "already
// applied"). Mirrors the read-modify-write pattern of setAppliedConfigs.
async function clearAppliedConfig(plugin, indexName) {
	const stored = await settings.getOne(plugin.id, 'appliedEmbedders') || {};
	delete stored[indexName];
	await settings.set(plugin.id, { appliedEmbedders: stored }, true);
}

module.exports = {
	EMBEDDER_NAME,
	SEMANTIC_SETTING_KEYS,
	isSemanticSearchEnabled,
	getSemanticRatio,
	getSemanticScoreThreshold,
	buildEmbedderConfig,
	validateEmbedderConfig,
	deepEqual,
	getAppliedConfig,
	setAppliedConfigs,
	resetAppliedConfigs,
	clearAppliedConfig,
};
