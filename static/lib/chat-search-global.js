'use strict';

window.chatSearchState = window.chatSearchState || {
	query: '',
	resultsHtml: '',
	isOpen: false,
	lastScroll: 0,
};

$(document).ready(function () {
	let observer = null;
	let debounceTimer = null;
	let searchSeq = 0;
	const SEARCH_DEBOUNCE_MS = 350;

	// Coexistence with nodebb-plugin-chat-search:
	// Both plugins share the same #global-chat-search-container ID and window.chatSearchState
	// global. When both are active, chat-search's script runs first (bundled earlier) and
	// injects its container with hardcoded colors. Our action:ajaxify.end handler fires second,
	// detects the foreign container (lacks data-plugin="meilisearch"), removes it, and injects
	// our own with theme-aware CSS variables + our socket handler bindings. Chat-search's
	// event handlers are left on removed DOM elements (dead references, no interference).
	// The window.chatSearchState object is reset identically by both scripts on non-chat
	// navigation, so no mutation conflicts occur.

	$(window).on('action:ajaxify.end', function (ev, data) {
		if (observer) observer.disconnect();
		const isChatUrl = data.url && data.url.match(/^(user\/[^/]+\/)?chats/);
		const isChatTemplate = data.template && (data.template.name === 'chats' || data.template === 'chats');

		if ((isChatUrl || isChatTemplate) && config.globalChatSearchEnabled !== false) {
			initSearchInjection();
		} else {
			window.chatSearchState = { query: '', resultsHtml: '', isOpen: false, lastScroll: 0 };
		}
	});

	$(window).on('action:chat.loaded', function () {
		highlightActiveChat();
	});

	if (ajaxify.data.template && (ajaxify.data.template.name === 'chats' || ajaxify.data.template === 'chats') && config.globalChatSearchEnabled !== false) {
		initSearchInjection();
	}

	function initSearchInjection() {
		const container = findContainer();
		if (container.length > 0) {
			injectSearchBar(container);
		} else {
			const targetNode = document.body;
			const config = { childList: true, subtree: true };
			observer = new MutationObserver(function () {
				const c = findContainer();
				if (c.length > 0) {
					injectSearchBar(c);
					observer.disconnect();
				}
			});
			observer.observe(targetNode, config);
		}
	}

	function findContainer() {
		let container = $('[component="chat/nav-wrapper"]');
		if (container.length === 0) container = $('.chats-page').find('.col-md-4').first();
		if (container.length === 0) container = $('[component="chat/list"]').parent();
		return container;
	}

	async function injectSearchBar(containerElement) {
		const container = containerElement || findContainer();
		if (container.length === 0) return false;
		if ($('#global-chat-search-container[data-plugin="meilisearch"]').length > 0) {
			// Already ours, just re-bind events (e.g. after ajaxify navigation).
			attachEvents();
			return true;
		}

		// Render the template FIRST (async), then atomically remove + prepend.
		// This prevents chat-search's MutationObserver from re-injecting its
		// container during the await gap (which caused duplicate search boxes).
		// Pre-translate placeholder (NodeBB 4.15.0 removed translator step from app.parseAndTranslate).
		const translator = await app.require('translator');
		const placeholder = await translator.translateKey('meilisearch:chatSearch.client.placeholder');
		const html = await app.parseAndTranslate('client/partials/chats/search-bar', { placeholder: placeholder });
		$('#global-chat-search-container').remove();
		container.prepend(html);
		restoreState();
		attachEvents();
		return true;
	}

	// injectSearchBar is async (uses app.parseAndTranslate). Callers don't await it —
	// that's fine because attachEvents binds by ID and restoreState reads from
	// window.chatSearchState, both available once prepended.

	function attachEvents() {
		$('#btn-chat-search').off('click').on('click', function () {
			clearTimeout(debounceTimer);
			executeSearch();
		});
		const input = $('#global-chat-search');
		input.off('keypress').on('keypress', function (e) {
			if (e.which === 13) {
				clearTimeout(debounceTimer);
				executeSearch();
			}
		});
		input.off('input').on('input', function () {
			window.chatSearchState.query = $(this).val();
			clearTimeout(debounceTimer);
			debounceTimer = setTimeout(executeSearch, SEARCH_DEBOUNCE_MS);
		});
		$('#global-search-results').off('scroll').on('scroll', function () {
			window.chatSearchState.lastScroll = $(this).scrollTop();
		});
	}

	function restoreState() {
		const input = $('#global-chat-search');
		const results = $('#global-search-results');
		if (window.chatSearchState.query) input.val(window.chatSearchState.query);
		if (window.chatSearchState.isOpen && window.chatSearchState.resultsHtml) {
			results.html(window.chatSearchState.resultsHtml).show();
			if ($.fn.timeago) results.find('.timeago').timeago();
			if (window.chatSearchState.lastScroll > 0) results.scrollTop(window.chatSearchState.lastScroll);
			highlightActiveChat();
		}
	}

	function cleanContent(content) {
		if (!content) return '';
		let text;
		try {
			const doc = new DOMParser().parseFromString(content, 'text/html');
			text = (doc.body && doc.body.textContent) || '';
		} catch (e) {
			text = content.replace(/<[^>]*>/g, ' ');
		}
		return text.replace(/\s+/g, ' ').trim();
	}

	async function executeSearch() {
		const query = $('#global-chat-search').val();
		const resultsContainer = $('#global-search-results');
		const translator = await app.require('translator');

		if (!query) {
			resultsContainer.hide();
			window.chatSearchState.isOpen = false;
			window.chatSearchState.resultsHtml = '';
			return;
		}

		const searchingLabel = await translator.translateKey('meilisearch:chatSearch.client.searching');
		const spinnerHtml = '<div class="text-center" style="padding:10px;"><i class="fa fa-spinner fa-spin"></i> ' + searchingLabel + '</div>';
		resultsContainer.show().html(spinnerHtml);
		window.chatSearchState.isOpen = true;

		const payload = { query: query };

		const seq = ++searchSeq;
		socket.emit('plugins.meilisearch.chatSearchGlobal', payload, async function (err, messages) {
			if (seq !== searchSeq) return;
			if (err) {
				const errorLabel = await translator.translateKey('meilisearch:chatSearch.client.error');
				const errorHtml = '<div class="alert alert-danger" style="margin:5px;">' + errorLabel + '</div>';
				resultsContainer.html(errorHtml);
				return;
			}
			if (!messages || messages.length === 0) {
				const noResultsLabel = await translator.translateKey('meilisearch:chatSearch.client.no-results');
				const noResHtml = '<div class="text-center" style="padding:10px; color: var(--bs-secondary-color);">' + noResultsLabel + '</div>';
				resultsContainer.html(noResHtml);
				window.chatSearchState.resultsHtml = noResHtml;
				return;
			}

			// Pre-translate unknown-user fallback (NodeBB 4.15.0 removed translator step from app.parseAndTranslate).
			const unknownUserLabel = await translator.translateKey('meilisearch:chatSearch.client.unknown-user');

			// Pre-process messages for the template
			const preparedMessages = messages.map(function (msg) {
				var ts = parseInt(msg.timestamp, 10);
				var isoTime = '';
				if (ts) {
					try { isoTime = new Date(ts).toISOString(); } catch (e) { isoTime = ''; }
				}

				var mid = parseInt(msg.mid, 10);
				var roomId = parseInt(msg.roomId, 10);
				var rank = parseInt(msg.rank, 10);
				var hasRank = rank >= 0 && app.user && app.user.userslug;
				var chatLink = hasRank
					? (config.relative_path || '') + '/user/' + app.user.userslug + '/chats/' + roomId + '/' + (rank + 1)
					: (config.relative_path || '') + '/message/' + mid;
				var senderName = (msg.user && msg.user.username) ? msg.user.username : unknownUserLabel;

				return {
					mid: mid,
					roomId: roomId,
					chatLink: chatLink,
					roomName: msg.roomName || '',
					mainParticipant: (msg.participants && msg.participants[0]) || null,
					user: msg.user,
					senderName: senderName,
					cleanedContent: cleanContent(msg.content),
					isoTime: isoTime,
				};
			});

			const html = await app.parseAndTranslate('client/partials/chats/search-result', { messages: preparedMessages });
			resultsContainer.html(html);

			if ($.fn.timeago) {
				resultsContainer.find('.timeago').timeago();
			}

			window.chatSearchState.resultsHtml = resultsContainer[0].innerHTML;
			window.chatSearchState.lastScroll = 0;
			highlightActiveChat();
		});
	}

	function highlightActiveChat() {
		let currentRoomId = ajaxify.data.roomId;
		if (!currentRoomId) {
			const match = window.location.pathname.match(/chats\/(\d+)/);
			if (match) currentRoomId = match[1];
		}
		if (!currentRoomId) return;
		$('.search-result').removeClass('active');
		const activeItem = $('.search-result[data-roomid="' + currentRoomId + '"]');
		activeItem.addClass('active');
	}
});
