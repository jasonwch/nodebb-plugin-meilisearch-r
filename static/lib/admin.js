'use strict';

/*
	This file is located in the "modules" block of plugin.json
	It is only loaded when the user navigates to /admin/plugins/meilisearch page
	It is not bundled into the min file that is served on the first load of the page.
*/
define('admin/plugins/meilisearch', [
	'settings',
	'api',
	'alerts',
	'modals',
], (settings, api, alerts, modals) => {
	const ACP = {};
	let reindexCompleteShown = false;

	function onReindex(data) {
		if (!data || !data.topic_progress || !data.post_progress || !data.message_progress) {
			return;
		}
		if (data.running) {
			reindexCompleteShown = false;
		}
		const progressBarContainers = document.querySelectorAll('.reindex-progress-container');
		const hasProgress = data.running ||
			Number(data.topic_progress.total) > 0 || Number(data.post_progress.total) > 0 || Number(data.message_progress.total) > 0;
		if (data.running || hasProgress) {
			progressBarContainers.forEach((container) => {
				container.classList.remove('hidden');
			});
			const topicProgressBar = document.getElementById('topic-reindex-progress');
			setProgress(topicProgressBar, data.topic_progress.current, data.topic_progress.total);
			const postProgressBar = document.getElementById('post-reindex-progress');
			setProgress(postProgressBar, data.post_progress.current, data.post_progress.total);
			const messageProgressBar = document.getElementById('message-reindex-progress');
			setProgress(messageProgressBar, data.message_progress.current, data.message_progress.total);

			document.getElementById('topic-reindex-progress-text').innerText =
				`${Number(data.topic_progress.current) || 0}/${Number(data.topic_progress.total) || 0}`;
			document.getElementById('post-reindex-progress-text').innerText =
				`${Number(data.post_progress.current) || 0}/${Number(data.post_progress.total) || 0}`;
			document.getElementById('message-reindex-progress-text').innerText =
				`${Number(data.message_progress.current) || 0}/${Number(data.message_progress.total) || 0}`;
		} else {
			progressBarContainers.forEach((container) => {
				container.classList.add('hidden');
			});
		}
		if (
			!data.running && data.topic_progress.total === data.topic_progress.current &&
			data.post_progress.total === data.post_progress.current &&
			data.message_progress.total === data.message_progress.current &&
			!reindexCompleteShown
		) {
			reindexCompleteShown = true;
			alerts.alert({
				title: '[[meilisearch:admin.reindexingCompleted]]',
				message: '[[meilisearch:admin.reindexingCompletedBody]]',
				type: 'success',
				timeout: 5000,
			});
			setTimeout(() => {
				progressBarContainers.forEach((container) => {
					container.classList.add('hidden');
				});
			}, 60000);
		}
	}

	// Suggestions only - the model field is always free text so any model name/tag works.
	// First entry in each list doubles as the input's placeholder.
	const SEMANTIC_MODEL_SUGGESTIONS = {
		openAi: ['text-embedding-3-small', 'text-embedding-3-large', 'text-embedding-ada-002'],
		huggingFace: [
			'BAAI/bge-base-en-v1.5', 'BAAI/bge-small-en-v1.5', 'BAAI/bge-large-en-v1.5',
			'sentence-transformers/all-MiniLM-L6-v2', 'sentence-transformers/all-mpnet-base-v2',
		],
		ollama: ['nomic-embed-text', 'mxbai-embed-large', 'all-minilm', 'bge-m3'],
		rest: [],
	};
	const SEMANTIC_URL_PLACEHOLDERS = {
		// Leave OpenAI's own default endpoint as an empty placeholder - it's optional there,
		// only needed to point at an OpenAI-compatible provider (OpenRouter, Azure, LocalAI...).
		openAi: 'https://api.openai.com/v1/embeddings',
		ollama: 'http://localhost:11434/api/embeddings',
		rest: 'https://api.example.com/embeddings',
	};
	// The literal {{text}}/{{embedding}} tokens below are Meilisearch's own REST
	// embedder template syntax, not benchpress - kept in JS (not the .tpl) so they
	// aren't mistaken for template expressions by the admin page's own renderer.
	const SEMANTIC_REST_REQUEST_PLACEHOLDER = '{"input": "{{text}}", "model": "text-embedding-3-small"}';
	const SEMANTIC_REST_RESPONSE_PLACEHOLDER = '{"data": [{"embedding": "{{embedding}}"}]}';
	const SEMANTIC_FIELDS_BY_PROVIDER = {
		openAi: ['apiKey', 'model', 'url'],
		huggingFace: ['model'],
		ollama: ['apiKey', 'model', 'url'],
		rest: ['apiKey', 'url', 'rest'],
	};
	// Mirrors the server's SEMANTIC_SETTING_KEYS (lib/embedder.js) minus "enabled" (handled
	// separately below, since off->on matters regardless of these) and "ratio" (confirmed
	// cost-free - buildEmbedderConfig() never reads it). Any of these changing while semantic
	// search is enabled makes the next Save re-embed every document through the configured
	// (often paid) API - see admin.semanticSearchSaveNotice.
	const SEMANTIC_COST_FIELDS = [
		'semanticSearchProvider', 'semanticSearchApiKey', 'semanticSearchModel',
		'semanticSearchUrl', 'semanticSearchDimensions', 'semanticSearchRestRequest', 'semanticSearchRestResponse',
	];
	// Snapshotted right after the form is populated (on load, and again after every
	// successful save) so saveSettings() can tell "did a cost-triggering field actually
	// change" apart from "the form just happens to hold these values" (e.g. re-saving
	// unrelated settings like ranking rules should never prompt a confirm).
	let semanticSnapshot = null;

	function captureSemanticSnapshot() {
		semanticSnapshot = { enabled: $('#semanticSearchEnabled').is(':checked'), fields: {} };
		SEMANTIC_COST_FIELDS.forEach((id) => {
			semanticSnapshot.fields[id] = $(`#${id}`).val();
		});
	}

	// True only when semantic search is (or is about to be) enabled AND something that
	// feeds buildEmbedderConfig() actually changed since the last load/save - never fires
	// for a plain re-save of the same values, or for disabling semantic search (free).
	function semanticCostChangeDetected() {
		if (!semanticSnapshot) return false;
		const enabledNow = $('#semanticSearchEnabled').is(':checked');
		if (!enabledNow) return false;
		if (!semanticSnapshot.enabled) return true;
		return SEMANTIC_COST_FIELDS.some(id => $(`#${id}`).val() !== semanticSnapshot.fields[id]);
	}

	ACP.init = function () {
		app.enterRoom('admin/plugins/meilisearch');
		settings.load('meilisearch', $('.meilisearch-settings'), () => {
			toggleLimitNotice();
			initSemanticSearchFields();
			toggleSemanticSearch();
			toggleForceReindexCostNotice();
			captureSemanticSnapshot();
		});
		$('#save').on('click', saveSettings);
		$('#reindex').on('click', reindex);
		formatLocalTimes();
		$('#globalChatSearchEnabled').on('change', toggleLimitNotice);
		$('#semanticSearchEnabled').on('change', () => { toggleSemanticSearch(); toggleForceReindexCostNotice(); });
		$('#semanticSearchProvider').on('change', toggleSemanticSearch);
		$('#semanticSearchRatio').on('input', updateSemanticRatioLabel);
		$('#force-reindex').on('change', toggleForceReindexCostNotice);
		socket.removeListener('plugins.meilisearch.reindex', onReindex);
		socket.on('plugins.meilisearch.reindex', onReindex);
		socket.removeListener('plugins.meilisearch.alert', onAlert);
		socket.on('plugins.meilisearch.alert', onAlert);
	};

	// Real-time server-side failures (embedder push rejected, search degraded because the
	// configured embedder/endpoint is broken, ...) - surfaced here instead of only in the
	// server log, since admins reading the ACP won't necessarily be tailing logs.
	function onAlert(data) {
		if (!data) return;
		alerts.alert({
			type: data.type || 'danger',
			title: data.titleKey || '[[meilisearch:admin.meilisearchError]]',
			message: data.message || '',
			timeout: 15000,
		});
	}

	function toggleLimitNotice() {
		$('#globalChatSearchLimitNotice').toggle($('#globalChatSearchEnabled').is(':checked'));
	}

	function toggleForceReindexCostNotice() {
		const semanticEnabled = $('#semanticSearchEnabled').is(':checked');
		const forced = $('#force-reindex').is(':checked');
		$('#semanticForceReindexCostNotice').toggle(semanticEnabled && forced);
	}

	function initSemanticSearchFields() {
		updateModelSuggestions($('#semanticSearchProvider').val());
		$('#semanticSearchUrl').attr('placeholder', SEMANTIC_URL_PLACEHOLDERS[$('#semanticSearchProvider').val()] || '');
		$('#semanticSearchRestRequest').attr('placeholder', SEMANTIC_REST_REQUEST_PLACEHOLDER);
		$('#semanticSearchRestResponse').attr('placeholder', SEMANTIC_REST_RESPONSE_PLACEHOLDER);
		updateSemanticRatioLabel();
	}

	// Model stays a free-text input for every provider - the datalist is only
	// suggestions, so any custom model name/tag can still be typed in directly.
	function updateModelSuggestions(provider) {
		const suggestions = SEMANTIC_MODEL_SUGGESTIONS[provider] || [];
		const $list = $('#semanticSearchModelList');
		$list.empty();
		suggestions.forEach((model) => {
			$list.append($('<option></option>').attr('value', model));
		});
		$('#semanticSearchModel').attr('placeholder', suggestions[0] || '');
	}

	function updateSemanticRatioLabel() {
		const val = parseFloat($('#semanticSearchRatio').val());
		$('#semanticSearchRatioValue').text(Number.isFinite(val) ? val.toFixed(2) : '0.50');
	}

	function toggleSemanticSearch() {
		const enabled = $('#semanticSearchEnabled').is(':checked');
		$('#semantic-search-options').toggle(enabled);
		if (!enabled) return;
		const provider = $('#semanticSearchProvider').val();
		const visibleFields = SEMANTIC_FIELDS_BY_PROVIDER[provider] || [];
		updateModelSuggestions(provider);
		$('#semanticSearchUrl').attr('placeholder', SEMANTIC_URL_PLACEHOLDERS[provider] || '');
		$('[data-semantic-field]').each(function toggleField() {
			const $field = $(this);
			$field.toggle(visibleFields.includes($field.attr('data-semantic-field')));
		});
	}

	function setProgress(element, current, total) {
		current = Number(current) || 0;
		total = Number(total) || 0;
		const pct = total > 0 ? Math.min(100, Math.max(0, Math.round(100 * current / total))) : 0;
		element.innerText = `${current}/${total}`;
		element.setAttribute('aria-valuenow', current);
		element.setAttribute('aria-valuemax', total);
		element.style.width = `${pct}%`;
	}

	function formatLocalTimes() {
		document.querySelectorAll('[data-finished-at]').forEach((el) => {
			const ts = Number(el.getAttribute('data-finished-at'));
			if (Number.isFinite(ts)) {
				el.textContent = new Date(ts).toLocaleString('en-US');
			}
		});
	}

	function saveSettings() {
		if (semanticCostChangeDetected()) {
			modals.confirm('[[meilisearch:admin.confirmSemanticSave]]', (confirm) => {
				if (confirm) doSaveSettings();
			});
			return;
		}
		doSaveSettings();
	}

	function doSaveSettings() {
		settings.save('meilisearch', $('.meilisearch-settings'), () => {
			const saveBtn = $('#save').get(0);
			if (saveBtn) {
				saveBtn.classList.toggle('saved', true);
				setTimeout(() => { saveBtn.classList.toggle('saved', false); }, 1500);
			}
			// Re-baseline: this save's values are now "current", so the next save only
			// prompts again if something changes relative to what was JUST saved.
			captureSemanticSnapshot();
			alerts.alert({
				type: 'success',
				alert_id: 'meilisearch-saved',
				title: '[[meilisearch:admin.settingsSaved]]',
				message: '[[meilisearch:admin.settingsSavedBody]]',
				timeout: 5000,
			});
			api.get('/plugins/meilisearch/reindex').then((response) => {
				if (response && !response.healthy) {
				alerts.alert({
					type: 'warning',
					alert_id: 'meilisearch-health',
					title: '[[meilisearch:admin.meiliUnreachable]]',
					message: '[[meilisearch:admin.meiliUnreachableBody]]',
					timeout: 10000,
				});
				}
			}).catch(() => {});
		});
	}
	function reindex() {
		const forceReindex = document.getElementById('force-reindex').checked;
		const semanticEnabled = $('#semanticSearchEnabled').is(':checked');
		const confirmMessage = (forceReindex && semanticEnabled)
			? '[[meilisearch:admin.confirmReindex]]<br><br><strong>[[meilisearch:admin.semanticForceReindexCostNotice]]</strong>'
			: '[[meilisearch:admin.confirmReindex]]';
		modals.confirm(confirmMessage, (confirm) => {
			if (!confirm) {
				return;
			}
			const request = !forceReindex
				? api.post('/plugins/meilisearch/reindex')
				: api.del('/plugins/meilisearch/reindex');
			request.then(() => {
				document.getElementById('index-action').scrollIntoView({ behavior: 'smooth' });
				alerts.alert({
					type: 'info',
					alert_id: 'meilisearch-reindex-started',
					title: '[[meilisearch:admin.indexingStarted]]',
					message: '[[meilisearch:admin.indexingStartedBody]]',
					timeout: 5000,
				});
			}).catch((err) => {
				alerts.alert({
					type: 'danger',
					alert_id: 'meilisearch-reindex-started',
					title: '[[meilisearch:admin.indexingStarted]]',
					message: err && err.message ? err.message : String(err),
				});
			});
		});
	}
	return ACP;
});
