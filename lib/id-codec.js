'use strict';

// Meilisearch document identifiers may only contain [a-zA-Z0-9_-] (max 511 bytes).
// Federated (ActivityPub) ids are URLs — remote post pids and remote chat mids can
// never be valid identifiers — so those documents are indexed under a reversible
// "apm_" + base64url encoding of their id (base64url's alphabet is exactly the
// allowed identifier alphabet), decoded back to the DB key when search results are
// returned to NodeBB. Local (numeric) ids already match the identifier rules and
// pass through verbatim, so an index holds numbers for local documents and
// "apm_…" strings for remote ones — unambiguous by construction.
//
// Remote TOPICS need no codec: core assigns them locally-generated UUID tids
// (src/topics/create.js routes non-numeric tids into topicsRemote:tid), and a
// UUID's charset is already identifier-safe — the topic index's primary key stays
// the real DB tid.

const PREFIX = 'apm_';

function encodeId(id) {
	if (id == null) return id;
	const asString = String(id);
	if (/^[a-zA-Z0-9_-]{1,511}$/.test(asString)) {
		return id; // already a valid identifier (local numeric ids, hashes, UUIDs)
	}
	const encoded = PREFIX + Buffer.from(asString, 'utf8').toString('base64url');
	// 511-byte identifier cap: ~380-char URL headroom. A longer one cannot be a
	// valid identifier either way — return it and let Meilisearch reject it loudly.
	return encoded.length <= 511 ? encoded : id;
}

function decodeId(id) {
	if (id == null || typeof id !== 'string' || !id.startsWith(PREFIX)) {
		return id; // local numeric ids (number or plain string), remote UUID tids
	}
	try {
		return Buffer.from(id.slice(PREFIX.length), 'base64url').toString('utf8');
	} catch (err) {
		return id;
	}
}

// Call-site-readable aliases: the same codec backs the chat index (mids) and the
// post index (pids).
const encodeMid = encodeId;
const decodeMid = decodeId;
const encodePid = encodeId;
const decodePid = decodeId;

module.exports = { encodeId, decodeId, encodeMid, decodeMid, encodePid, decodePid };
