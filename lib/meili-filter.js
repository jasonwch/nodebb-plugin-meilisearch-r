'use strict';

// Meilisearch filter expressions: numeric values are emitted bare (numeric
// comparison), everything else as a double-quoted string literal with `\` and
// `"` escaped — unquoted non-numeric values are a filter syntax error.
//
// Federated content makes this mandatory: remote topic tids are UUID strings and
// remote author uids are actor URLs, and both are filterable post-index
// attributes. Emitting them bare (the pre-federation code path parseInt-ed them
// and silently DROPPED the filter — an in-topic search then leaked results from
// every topic) is a correctness bug, not just a syntax one.
//
// The numeric fast path must stay: local tids/uids/cids are indexed as numbers,
// and a quoted "123" in Meilisearch compares as a string literal that never
// matches a numerically-stored value.

const NUMERIC_RE = /^[+-]?\d+(\.\d+)?$/;

function filterValue(value) {
	const asString = String(value);
	if (asString !== '' && NUMERIC_RE.test(asString)) {
		return asString;
	}
	return `"${asString.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
}

module.exports = { filterValue };
