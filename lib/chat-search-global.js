'use strict';

const winston = nodebb.require('winston');
const settings = nodebb.require('./src/meta/settings');
const Messaging = nodebb.require('./src/messaging');
const user = nodebb.require('./src/user');
const translator = nodebb.require('./src/translator');
const db = nodebb.require('./src/database');
const socketPlugins = nodebb.require('./src/socket.io/plugins');
const { resolveMinTermLength } = require('./settings-helpers');
const { EMBEDDER_NAME, isSemanticSearchEnabled, getSemanticRatio, getSemanticScoreThreshold } = require('./embedder');
const { safeSearch } = require('./search');
const { decodeMid } = require('./id-codec');
const {
	GLOBAL_CHAT_SEARCH_LIMIT, GLOBAL_CHAT_SEARCH_FETCH, MEMBERSHIP_CHUNK_SIZE, DECORATION_BATCH_SIZE,
} = require('./constants');

async function translateMeili(language, key, ...args) {
	return await translator.translate(
		translator.compile(`meilisearch:${key}`, ...args),
		language,
	);
}

// Attaches room name, participants, and sender metadata to each message for the client UI.
// F6: hydration is bounded to what the UI actually renders (display name, avatar,
// "and N more") — the previous implementation fetched EVERY member of the room
// (getUidsInRoom(0, -1)) plus a full getUsersFields hydrate just to derive them.
// The chat:room:{roomId}:uids zset is scored by join timestamp (ascending), so a
// small window from rank 0 selects the same earliest joiners the full scan did.
async function decorateRoomMatches({ matches, roomId, targetUid, userLang }) {
	const roomUidsKey = `chat:room:${roomId}:uids`;
	const [memberCount, selfIsMember] = await Promise.all([
		db.sortedSetCard(roomUidsKey),
		db.isSortedSetMember(roomUidsKey, targetUid),
	]);
	const otherCount = Math.max(0, memberCount - (selfIsMember ? 1 : 0));

	let displayName = '';
	let firstOther = null;
	if (otherCount === 0) {
		displayName = await translateMeili(userLang, 'chatSearch.room.self-chat');
	} else {
		// Up to 2 other members are rendered (names or "first two + N more");
		// +1 slot covers the searcher themselves landing inside the window.
		const needed = Math.min(otherCount, 2) + (selfIsMember ? 1 : 0);
		const windowUids = await db.getSortedSetRange(roomUidsKey, 0, needed - 1);
		const usersData = await user.getUsersFields(windowUids, ['uid', 'username', 'picture', 'icon:text', 'icon:bgColor']);
		const otherUsers = usersData.filter(u => parseInt(u.uid, 10) !== parseInt(targetUid, 10));
		firstOther = otherUsers[0] || null;
		// Claim-1 fix: branch on the TRUE other-count, never the windowed array
		// length — when the searcher is among the first 3 joiners the window holds
		// only 2 others (their own slot consumes one), which wrongly took the
		// "no more users" branch and dropped the "and N more users" suffix.
		if (otherCount <= 2) {
			displayName = otherUsers.map(u => u.username).join(', ');
		} else {
			const firstTwo = otherUsers.slice(0, 2).map(u => u.username).join(', ');
			const remaining = otherCount - 2;
			displayName = await translateMeili(userLang, 'chatSearch.room.and-more-users', firstTwo, remaining);
		}
	}

	const roomData = await Messaging.getRoomData(roomId);
	const roomName = (roomData && roomData.roomName) || displayName;

	// Batch-fetch sorted-set ranks for all matched mids.
	// Core's /message/:mid redirect drops the index segment when rank is 0 (falsy),
	// so we attach the rank here and construct the chat URL client-side with rank+1
	// to always include a truthy 1-based index segment.
	//
	// Drift compensation: core's Messaging.getMessages() derives each rendered
	// message's data-index from the room's messageCount field (count - start -
	// windowPos - 1), NOT from live sorted-set positions. purgeMessage() (added in
	// v4.16.0, PR #14678) removes mids without decrementing messageCount, so the
	// rendered data-index = trueRank + (messageCount - cardinality). Add the drift
	// so search links match what the renderer actually highlights. When core fixes
	// the invariant (decrement on purge / recount), drift becomes 0 and this is a
	// no-op — remove this compensation then.
	const midsForRank = matches.map(m => m.mid || m.messageId);
	const midsKey = `chat:room:${roomId}:mids`;
	const [ranks, cardinality] = await Promise.all([
		db.sortedSetRanks(midsKey, midsForRank),
		db.sortedSetCard(midsKey),
	]);
	const messageCount = parseInt(roomData && roomData.messageCount, 10); // NaN when roomData null → guard below
	const drift = Number.isNaN(messageCount) ? 0 : messageCount - cardinality;

	matches.forEach((m, i) => {
		if (!m.mid && m.messageId) m.mid = m.messageId;
		if (!m.roomId) m.roomId = roomId;
		if (!m.user || !m.user.username) {
			m.user = m.fromUser || { username: 'Unknown', 'icon:text': '?', 'icon:bgColor': 'var(--bs-tertiary-bg)' };
		}
		m.roomName = roomName;
		m.participants = firstOther ? [firstOther] : [];
		m.rank = ranks[i] === null ? null : ranks[i] + drift;
	});

	return matches;
}

// Global chat search (across all of the user's rooms) plus the config flag that
// enables/disables it client-side.
module.exports = function attachChatSearchGlobal(plugin) {
	// Global chat search: queries the chat_message Meilisearch index across ALL rooms
	// the user belongs to (unlike the per-room searchMessages which is scoped to one roomId).
	// Uses 1.5× over-fetch (300) to compensate for join-timestamp/deleted-message filtering,
	// then renders only surviving hits through NodeBB's getMessagesData pipeline.
	// Results are sorted by message timestamp (newest first).
	plugin.chatSearchGlobal = async function (socket, data) {
		data = data || {};
		if (!socket.uid) throw new Error('Not logged in');
		const chatSearchEnabled = await settings.getOne(plugin.id, 'globalChatSearchEnabled');
		if (chatSearchEnabled === false || chatSearchEnabled === 'off' || chatSearchEnabled === 'false') {
			return [];
		}
		if (!plugin.healthy && !(await plugin.checkHealth())) {
			throw new Error('MeiliSearch is not healthy');
		}
		const targetUid = socket.uid;
		const userSettings = await user.getSettings(socket.uid);
		const userLang = userSettings.userLang || 'en-GB';

		const query = String(data.query || '').trim();
		if (!query) {
			return [];
		}

		let roomIds = await db.getSortedSetRevRange('uid:' + targetUid + ':chat:rooms', 0, -1);
		// Chunked membership check with event loop yields to prevent blocking on large room lists.
		// Each chunk fires hooks synchronously inside Promise.all (~0.6ms per 100 rooms);
		// setImmediate between chunks lets other users' I/O callbacks run.
		let inRoom = [];
		for (let i = 0; i < roomIds.length; i += MEMBERSHIP_CHUNK_SIZE) {
			const chunk = roomIds.slice(i, i + MEMBERSHIP_CHUNK_SIZE);
			inRoom = inRoom.concat(await Messaging.isUserInRoom(targetUid, chunk));
			await new Promise(resolve => setImmediate(resolve));
		}
		roomIds = roomIds.filter((roomId, idx) => inRoom[idx]);
		if (!roomIds.length) {
			return [];
		}

		const minTermLength = await resolveMinTermLength(plugin);
		if (!query.split(' ').some(word => word.length >= minTermLength)) {
			return [];
		}

		const filter = [roomIds.map(rid => `roomId = ${parseInt(rid, 10)}`)];
		winston.debug(`[plugin/meilisearch] Global chat search for "${query}" in ${roomIds.length} rooms`);
		const semanticExtras = (await isSemanticSearchEnabled(plugin))
			? {
				hybrid: { embedder: EMBEDDER_NAME, semanticRatio: await getSemanticRatio(plugin) },
				rankingScoreThreshold: await getSemanticScoreThreshold(plugin),
			}
			: {};
		const result = await safeSearch(plugin, 'chat_message', query, {
			attributesToRetrieve: ['mid', 'roomId', 'uid', 'timestamp'],
			limit: GLOBAL_CHAT_SEARCH_FETCH,
			filter: filter,
			matchingStrategy: 'last',
			...semanticExtras,
		});

		if (!result || !result.hits || !result.hits.length) {
			return [];
		}

		// Batch-fetch room public flags (1 query) + join timestamps for private rooms (N queries).
		const hitRoomIds = [...new Set(result.hits.map(h => parseInt(h.roomId, 10)))];
		const roomData = await db.getObjectsFields(
			hitRoomIds.map(rid => `chat:room:${rid}`),
			['public'],
		);
		const roomIsPublic = {};
		hitRoomIds.forEach((rid, i) => {
			roomIsPublic[rid] = parseInt(roomData[i] && roomData[i].public, 10) === 1;
		});

		const privateRoomIds = hitRoomIds.filter(rid => !roomIsPublic[rid]);
		const joinTimestamps = {};
		if (privateRoomIds.length) {
			// F6: one bulk call instead of one db.sortedSetScore per room (N+1).
			const scores = await db.sortedSetsScore(privateRoomIds.map(rid => `chat:room:${rid}:uids`), targetUid);
			privateRoomIds.forEach((rid, i) => {
				joinTimestamps[rid] = scores[i] || 0;
			});
		}

		// Filter + sort by timestamp (newest first) + slice to limit.
		const visibleHits = result.hits
			.filter((hit) => {
				const rid = parseInt(hit.roomId, 10);
				if (roomIsPublic[rid]) return true;
				const ts = parseInt(hit.timestamp, 10);
				return ts >= (joinTimestamps[rid] || 0);
			})
			.sort((a, b) => parseInt(b.timestamp, 10) - parseInt(a.timestamp, 10))
			.slice(0, GLOBAL_CHAT_SEARCH_LIMIT);

		if (!visibleHits.length) {
			return [];
		}

		// Remote (URL) mids are stored under their reversible codec form — decode
		// back to the DB key before Messaging.getMessagesData / room grouping.
		visibleHits.forEach((hit) => {
			hit.mid = decodeMid(hit.mid);
		});

		// Group by room for decoration, then reassemble in timestamp order via mid→message map.
		const hitsByRoom = {};
		visibleHits.forEach((hit) => {
			const rid = parseInt(hit.roomId, 10);
			if (!hitsByRoom[rid]) hitsByRoom[rid] = [];
			hitsByRoom[rid].push(hit.mid);
		});

		// Process rooms in bounded-concurrency batches (10 at a time) to avoid
		// saturating the MongoDB connection pool (maxPoolSize: 20).
		const roomIdsWithHits = Object.keys(hitsByRoom);
		const decoratedMap = {};
		for (let i = 0; i < roomIdsWithHits.length; i += DECORATION_BATCH_SIZE) {
			const batch = roomIdsWithHits.slice(i, i + DECORATION_BATCH_SIZE);
			const batchResults = await Promise.all(batch.map(async (ridStr) => {
				const rid = parseInt(ridStr, 10);
				const mids = hitsByRoom[rid];
				const messages = await Messaging.getMessagesData(mids, targetUid, rid, false);
				if (!messages || !messages.length) return [];
				return decorateRoomMatches({ matches: messages, roomId: rid, targetUid, userLang });
			}));
			batchResults.forEach((decorated) => {
				decorated.forEach((msg) => {
					const mid = msg.mid || msg.messageId;
					if (mid) decoratedMap[mid] = msg;
				});
			});
		}

		// Reassemble in pre-sorted timestamp order.
		const allResults = [];
		for (const hit of visibleHits) {
			const msg = decoratedMap[hit.mid];
			if (msg) allResults.push(msg);
		}

		return allResults;
	};

	// Register socket handler at module level per NodeBB 4.14.x convention
	// (src/socket.io/plugins.js docs: require and add listeners at load time).
	// The health check inside chatSearchGlobal guards against calls before init completes.
	socketPlugins.meilisearch = socketPlugins.meilisearch || {};
	socketPlugins.meilisearch.chatSearchGlobal = plugin.chatSearchGlobal;

	// Expose globalChatSearchEnabled to client config (standard filter:config.get pattern,
	// same as nodebb-plugin-emoji's emojiCustomFirst).
	plugin.addConfig = async function (config) {
		const enabled = await settings.getOne(plugin.id, 'globalChatSearchEnabled');
		config.globalChatSearchEnabled = enabled !== false && enabled !== 'off' && enabled !== 'false';
		return config;
	};
};
