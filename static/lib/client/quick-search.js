'use strict';

(function () {
	var DECORATED_FLAG = 'data-meilisearch-decorated';
	var HEADER_CLASS = 'meilisearch-quick-search-header';

	var _hooks = null;

	var PREFIX_ONLY_RE = /^in:topic-\d+\s*$/;
	var SEARCH_API_URL_RE = /\/api\/search(\?|$)/;

	function isComposerQuickSearch(options) {
		return options && options.searchOptions &&
			parseInt(options.searchOptions.composer, 10) === 1;
	}

	function hasResultPosts(data) {
		return data && data.posts && data.posts.length > 0;
	}

	function decorate(options) {
		var resultEl = options && options.searchElements && options.searchElements.resultEl;
		if (!resultEl || !resultEl.length) { return; }
		if (resultEl.attr(DECORATED_FLAG) === '1') { return; }

		require(['benchpress'], function (Benchpress) {
			if (!Benchpress || typeof Benchpress.render !== 'function') { return; }
			// re-check after async require in case another fire raced ahead
			if (resultEl.attr(DECORATED_FLAG) === '1') { return; }

			Benchpress.render('client/partials/quick-search-header', {})
				.then(function (html) {
					// re-check after async render+translate in case another fire raced ahead
					if (resultEl.attr(DECORATED_FLAG) === '1') { return; }
					var header = $(html);
					resultEl.find('.' + HEADER_CLASS).remove();
					resultEl.prepend(header);
					resultEl.attr(DECORATED_FLAG, '1');
					bindResultClick(resultEl);
				})
				.catch(function () {
					// template not available (e.g. build pending) - leave results undecorated
				});
		});
	}

	function bindResultClick(resultEl) {
		resultEl
			.off('click.meili-minimize')
			.on('click.meili-minimize', '.quick-search-results-container a', function () {
				var composerEl = resultEl.closest('.composer');
				if (!composerEl.length) { return; }
				var hideBtn = composerEl.find('[data-action="hide"]').first();
				if (!hideBtn.length) { return; }
				if (!_hooks) { return; }
				// Minimize AFTER ajaxify navigation completes so the in-flight
				// page transition isn't interrupted by composer.minimize().
				// hooks.one() self-unregisters after the first (and only) fire.
				_hooks.one('action:ajaxify.end', function () {
					hideBtn.trigger('click');
				});
			});
	}

	function onQuickSearchComplete(payload) {
		if (!isComposerQuickSearch(payload.options)) { return; }
		if (!hasResultPosts(payload.data)) { return; }
		decorate(payload.options);
	}

	function dismissVisible() {
		$('.quick-search-container').filter(':not(.hidden)')
			.filter('[' + DECORATED_FLAG + '="1"]')
			.each(function () {
				var $box = $(this);
				var composer = $box.closest('.composer');
				if (!composer.length) { return; }
				$box.addClass('hidden');
				// restore keyboard/mouse focus to the Subject field
				var titleInput = composer.find('input.title').first();
				if (titleInput.length) {
					titleInput[0].focus();
				}
			});
	}

	function onKeydown(e) {
		if (e.key !== 'Escape' && e.keyCode !== 27) { return; }
		var dismissed = false;
		$('.quick-search-container').filter(':not(.hidden)')
			.filter('[' + DECORATED_FLAG + '="1"]')
			.each(function () {
				dismissed = true;
				return false;
			});
		if (dismissed) {
			e.preventDefault();
			e.stopPropagation();
			dismissVisible();
		}
	}

	function patchSearchApi() {
		require(['search'], function (search) {
			if (!search || typeof search.api !== 'function') { return; }
			if (search.api._meiliWrapped) { return; }
			var originalApi = search.api;
			var wrapped = function (data, callback) {
				var term = String((data && data.term) || '');
				if (!term.trim() || PREFIX_ONLY_RE.test(term)) {
					callback({ posts: [], categories: [], matchCount: 0, pageCount: 1 });
					return;
				}
				return originalApi.call(search, data, callback);
			};
			wrapped._meiliWrapped = true;
			search.api = wrapped;
		});
	}

	function hideQuickSearchSpinners() {
		$('.quick-search-container').each(function () {
			var $box = $(this);
			$box.find('.loading-indicator').addClass('hidden');
			if (!$box.find('.quick-search-results-container').children().length) {
				$box.addClass('hidden');
			}
		});
	}

	function bindSearchFailGuard() {
		$(document).on('ajaxError.meili-quicksearch', function (event, jqXHR, settings) {
			if (!settings || !settings.url) { return; }
			if (!SEARCH_API_URL_RE.test(settings.url)) { return; }
			hideQuickSearchSpinners();
		});
	}

	function init() {
		if (!window.config || !window.config.searchEnabled) { return; }
		if (typeof window.jQuery === 'undefined') { return; }

		require(['hooks'], function (hooks) {
			_hooks = hooks;
			hooks.on('action:search.quick.complete', function (payload) {
				onQuickSearchComplete(payload || {});
			});

			document.addEventListener('keydown', onKeydown);
		});

		patchSearchApi();
		bindSearchFailGuard();
	}

	if (document.readyState === 'loading') {
		document.addEventListener('DOMContentLoaded', init);
	} else {
		init();
	}
})();
