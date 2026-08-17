'use strict';

const winston = nodebb.require('winston');
const settings = nodebb.require('./src/meta/settings');
const Messaging = nodebb.require('./src/messaging');
const user = nodebb.require('./src/user');
const translator = nodebb.require('./src/translator');
const db = nodebb.require('./src/database');
const socketPlugins = nodebb.require('./src/socket.io/plugins');
const { resolveMinTermLength } = require('./settings-helpers');
const { EMBEDDER_NAME, isSemanticSearchEnabled, getSemanticRatio } = require('./embedder');
const { safeSearch } = require('./search');
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
async function decorateRoomMatches({ matches, roomId, targetUid, userLang }) {
	const uids = await Messaging.getUidsInRoom(roomId, 0, -1);
	const usersData = await user.getUsersFields(uids, ['uid', 'username', 'picture', 'icon:text', 'icon:bgColor']);
	const otherUsers = usersData.filter(u => parseInt(u.uid, 10) !== parseInt(targetUid, 10));

	let displayName = '';
	if (otherUsers.length === 0) {
		displayName = await translateMeili(userLang, 'chatSearch.room.self-chat');
	} else if (otherUsers.length <= 2) {
		displayName = otherUsers.map(u => u.username).join(', ');
	} else {
		const firstTwo = otherUsers.slice(0, 2).map(u => u.username).join(', ');
		const remaining = otherUsers.length - 2;
		displayName = await translateMeili(userLang, 'chatSearch.room.and-more-users', firstTwo, remaining);
	}

	const roomData = await Messaging.getRoomData(roomId);
	const roomName = (roomData && roomData.roomName) || displayName;

	// Batch-fetch sorted-set ranks for all matched mids.
	// Core's /message/:mid redirect drops the index segment when rank is 0 (falsy),
	// so we attach the rank here and construct the chat URL client-side with rank+1
	// to always include a truthy 1-based index segment.
	const midsForRank = matches.map(m => m.mid || m.messageId);
	const ranks = await db.sortedSetRanks(`chat:room:${roomId}:mids`, midsForRank);

	matches.forEach((m, i) => {
		if (!m.mid && m.messageId) m.mid = m.messageId;
		if (!m.roomId) m.roomId = roomId;
		if (!m.user || !m.user.username) {
			m.user = m.fromUser || { username: 'Unknown', 'icon:text': '?', 'icon:bgColor': 'var(--bs-tertiary-bg)' };
		}
		m.roomName = roomName;
		m.participants = otherUsers.length ? [otherUsers[0]] : [];
		m.rank = ranks[i];
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
		const hybrid = (await isSemanticSearchEnabled(plugin))
			? { embedder: EMBEDDER_NAME, semanticRatio: await getSemanticRatio(plugin) }
			: undefined;
		const result = await safeSearch(plugin, 'chat_message', query, {
			attributesToRetrieve: ['mid', 'roomId', 'uid', 'timestamp'],
			limit: GLOBAL_CHAT_SEARCH_FETCH,
			filter: filter,
			matchingStrategy: 'last',
			hybrid,
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
			const scores = await Promise.all(
				privateRoomIds.map(rid => db.sortedSetScore(`chat:room:${rid}:uids`, targetUid)),
			);
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
