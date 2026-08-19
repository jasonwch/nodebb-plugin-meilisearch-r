'use strict';

// Credential redaction for log/alert paths.
//
// Background: M5 + C1 fix. Meilisearch task error messages and settings diff logs may
// echo the configured apiKey (e.g. "Unauthorized: key sk-abc123" or nested in JSON).
// Without redaction, these flow to:
//   - winston transports (stdout, logs/output.log, /admin/developer/logs)
//   - log aggregators (Loki/ELK/Datadog) which sit OUTSIDE the admin trust boundary
//   - admin ACP toasts (15s transient — already mitigated by Issue 2 admin-room filter)
//
// Strategy: dual-layer redaction
//   1. Pattern-based regex: catches known API key shapes (OpenAI sk-*, Bearer tokens)
//      even when we can't read the configured value (e.g. message logged before init)
//   2. Value-based substitution: reads semanticSearchApiKey from settings and string-
//      replaces any literal occurrence — catches custom REST tokens, future OpenAI
//      key formats, and exotic auth schemes that don't match the regex.
//
// Value-based redaction requires a settings.getOne DB read on first call; cached for
// 5 min so throttled error paths (notifyAdmins throttles 60s/key) incur ≤1 DB read/min.

const settings = nodebb.require('./src/meta/settings');

// Fields whose VALUES must never appear in winston diff logs (lib/settings.js isBreaking).
// Listed by ACP field name. semanticSearchUrl is intentionally NOT included — it's an
// internal hostname, not a credential, and admins need it for debugging connectivity.
const SECRET_SETTINGS = new Set([
	'semanticSearchApiKey',
	'semanticSearchRestRequest',
	'semanticSearchRestResponse',
]);

const REDACTION_PLACEHOLDER = '<redacted>';

// Patterns to catch common API key formats in free-form error strings.
// OpenAI keys: sk-*, sk-proj-*, sk-org-*, sk-svcacct-* — all start with "sk-"
// and are at least 20 chars long (real keys are 40+ but we redact aggressively at 20).
const KEY_PATTERNS = [
	/\bsk-[a-zA-Z0-9_-]{20,}\b/g,
	/\bBearer\s+[a-zA-Z0-9_-]{8,}\b/gi,
];

// Min length for value-based redaction — avoids replacing short strings that might
// legitimately appear in error messages (e.g. an apiKey of "ab" would corrupt many
// error strings). 8 chars is safely above any plausible short substring.
const MIN_VALUE_REDACT_LENGTH = 8;

// Cached configured semanticSearchApiKey to avoid hitting DB on every redact call.
// TTL ensures we pick up rotations within 5 min. Cached as the raw string value
// (or '' when no key configured) so we can distinguish "cached empty" from "not cached".
let _cachedApiKey = null;
let _cachedAt = 0;
const CACHE_TTL_MS = 5 * 60 * 1000;

async function getCachedApiKey(plugin) {
	const now = Date.now();
	if (_cachedApiKey !== null && now - _cachedAt < CACHE_TTL_MS) {
		return _cachedApiKey;
	}
	try {
		// A1 fix: trim the cached value to match the on-wire key (embedder.js get() trims
		// before sending to Meilisearch, so any echo in errors is the trimmed form).
		// Without this, a padded stored key (" sk-abc ") would defeat value-based redaction
		// when Meilisearch echoes the trimmed key ("sk-abc") in a 401 error.
		_cachedApiKey = ((await settings.getOne(plugin.id, 'semanticSearchApiKey')) || '').trim();
		_cachedAt = now;
	} catch (e) {
		// A2 fix: don't set _cachedAt on the failure path — let the next call retry immediately
		// instead of cycling through the full 5-min TTL with a stale empty cache. Pattern-based
		// redaction still runs, so sk-* and Bearer-shaped keys stay protected during the blip.
		_cachedApiKey = '';
	}
	return _cachedApiKey;
}

// Test-only hooks (exported for the test sandbox; not consumed by production code paths).
function _resetCache() {
	_cachedApiKey = null;
	_cachedAt = 0;
}

function _setCachedApiKey(value) {
	_cachedApiKey = (value || '').trim();
	_cachedAt = Date.now();
}

// Redact known secret patterns + the configured apiKey value from a free-form string.
// Used by winston log calls and notifyAdmins payloads so credentials don't leak
// when Meilisearch echoes them in error messages.
//
// Returns the redacted string. If `plugin` is omitted, only pattern-based redaction
// runs (used in early-init paths before plugin object is fully wired).
async function redactSecrets(message, plugin) {
	if (typeof message !== 'string' || !message) return message;
	let redacted = message;

	// Layer 1: pattern-based — catches known formats even without DB access
	for (const pattern of KEY_PATTERNS) {
		redacted = redacted.replace(pattern, REDACTION_PLACEHOLDER);
	}

	// Layer 2: value-based — catches the exact configured apiKey regardless of format
	if (plugin) {
		const configuredKey = await getCachedApiKey(plugin);
		if (configuredKey && configuredKey.length >= MIN_VALUE_REDACT_LENGTH) {
			// Escape regex special chars in the key (keys can contain +, /, = for base64)
			const escapedKey = configuredKey.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
			redacted = redacted.replace(new RegExp(escapedKey, 'g'), REDACTION_PLACEHOLDER);
		}
	}

	return redacted;
}

// Whole-value redaction for winston diff logs (lib/settings.js isBreaking).
// Returns '<redacted>' for credential-bearing fields, JSON.stringify for others.
// Preserves the "did it change?" audit signal without leaking the credential value.
function redactSettingForLog(setting, value) {
	if (SECRET_SETTINGS.has(setting)) return REDACTION_PLACEHOLDER;
	return JSON.stringify(value);
}

module.exports = {
	redactSecrets,
	redactSettingForLog,
	SECRET_SETTINGS,
	REDACTION_PLACEHOLDER,
	// Test-only exports (not consumed by production code paths):
	_resetCache,
	_setCachedApiKey,
};
