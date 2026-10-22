'use strict';

// NodeBB write-hook payloads carry deleted/system flags in different shapes:
// DB reads return '0'/'1' strings, but some hook payloads normalize to booleans —
// action:post.edit fires with postData.deleted = !!postData.deleted
// (src/posts/edit.js), so a bare parseInt(x, 10) === 1 silently passes on `true`
// and re-indexes soft-deleted content. One helper for every guard site.
function isFlagSet(value) {
	return value === true || value === 1 || value === '1';
}

module.exports = { isFlagSet };
