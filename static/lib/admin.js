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
	'translator',
], (settings, api, alerts, modals, translator) => {
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
	// The literal {{text}}/{{embedding}}/{{..}} tokens below are Meilisearch's own REST
	// embedder template syntax, not benchpress - kept in JS (not the .tpl) so they aren't
	// mistaken for template expressions by the admin page's own renderer. "{{..}}" as the
	// 2nd array element (in BOTH templates) enables batching - Meilisearch sends multiple
	// documents' texts in one HTTP call during indexing; omitting it causes
	// "response has a single embedding, but request has multiple texts to embed".
	const SEMANTIC_REST_REQUEST_PLACEHOLDER = '{"input": ["{{text}}", "{{..}}"], "model": "text-embedding-3-small"}';
	const SEMANTIC_REST_RESPONSE_PLACEHOLDER = '{"data": [{"embedding": "{{embedding}}"}, "{{..}}"]}';
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

	// Mirrors lib/embedder.js validateEmbedderConfig. Reads form values via jQuery
	// selectors (not nodebb.require — runs in browser) and applies the same per-provider
	// rules: openAi requires apiKey iff url blank; huggingFace/ollama need nothing;
	// rest requires url, warns on non-empty non-JSON request/response templates.
	//
	// Returns null if valid, or an error string (will be displayed via alerts.alert).
	function validateEmbedderForm() {
		const enabled = $('#semanticSearchEnabled').is(':checked');
		if (!enabled) return null;
		const provider = $('#semanticSearchProvider').val();
		// .trim() all text inputs — admin may paste with trailing whitespace/newline, which
		// would pass client-side "non-empty" check but fail at the embedding API (e.g. OpenAI
		// returns 401 for an apiKey with trailing whitespace).
		const apiKey = ($('#semanticSearchApiKey').val() || '').trim();
		const url = ($('#semanticSearchUrl').val() || '').trim();
		const restRequest = ($('#semanticSearchRestRequest').val() || '').trim();
		const restResponse = ($('#semanticSearchRestResponse').val() || '').trim();
		const missing = [];
		const invalid = [];
		switch (provider) {
			case 'openAi':
				// apiKey required only when url is blank (custom OpenAI-compatible endpoint may not need auth)
				if (!apiKey && !url) missing.push('apiKey');
				break;
			case 'huggingFace':
				// model + nothing required (HF runs in Meilisearch, defaults work)
				break;
			case 'ollama':
				// url + model + apiKey all optional (defaults work for local install)
				break;
			case 'rest':
				if (!url) missing.push('url');
				if (restRequest.trim() && !isValidJson(restRequest)) invalid.push('request');
				if (restResponse.trim() && !isValidJson(restResponse)) invalid.push('response');
				break;
			default:
				return `Unknown provider: ${provider}`;
		}
		if (!missing.length && !invalid.length) return null;
		const parts = [];
		if (missing.length) parts.push(`Required field(s) missing for "${provider}" provider: ${missing.join(', ')}`);
		if (invalid.length) parts.push(`Invalid JSON in field(s): ${invalid.join(', ')}`);
		return parts.join('; ');
	}

	function isValidJson(str) {
		try { JSON.parse(str); return true; } catch (e) { return false; }
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
		$('#semanticSearchScoreThreshold').on('input', updateSemanticScoreThresholdLabel);
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
		updateSemanticScoreThresholdLabel();
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

	function updateSemanticScoreThresholdLabel() {
		const val = parseFloat($('#semanticSearchScoreThreshold').val());
		$('#semanticSearchScoreThresholdValue').text(Number.isFinite(val) ? val.toFixed(2) : '0.20');
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
		// (A) Pre-flight validation: block save on missing required fields (config invalid).
		const validationError = validateEmbedderForm();
		if (validationError) {
			alerts.alert({
				type: 'danger',
				alert_id: 'meilisearch-validation',
				title: '[[meilisearch:admin.semanticConfigInvalid]]',
				message: validationError,
				timeout: 10000,
			});
			return;  // block save — doSaveSettings not called
		}
		// (Pure A — NEW) Ollama reachability probe. When semantic is enabled AND provider is
		// ollama, asynchronously test <url>/api/version from NodeBB's container (5s timeout).
		// On unreachable, show modal: "Ollama is not reachable from this NodeBB. If you are
		// sure the Ollama is up and reachable from Meilisearch instance, click OK to save anyway."
		// Admin can override because the test runs from NodeBB's network, not Meilisearch's
		// (different networks possible if Meilisearch is host-networked or different Docker net).
		// Other providers skip the probe and go straight to cost-confirm / save.
		const semanticEnabled = $('#semanticSearchEnabled').is(':checked');
		const provider = $('#semanticSearchProvider').val();
		if (semanticEnabled && provider === 'ollama') {
			proceedWithOllamaProbe();
			return;
		}
		// Non-ollama providers: cost-change confirm (if applicable) then save
		if (semanticCostChangeDetected()) {
			modals.confirm('[[meilisearch:admin.confirmSemanticSave]]', (confirm) => {
				if (confirm) doSaveSettings();
			});
			return;
		}
		doSaveSettings();
	}

	// Pure A: Async Ollama reachability probe. Shows "Testing Ollama..." info toast, then
	// calls POST /api/v3/meilisearch/test-ollama which makes a 5s HTTP GET to <url>/api/version
	// from NodeBB's container. On success, proceeds with the normal cost-confirm → save chain.
	// On unreachable, shows modals.confirm() with the admin-specified text — admin's "OK"
	// click means "Save anyway" (override the probe failure), "Cancel" aborts the save.
	async function proceedWithOllamaProbe() {
		const url = ($('#semanticSearchUrl').val() || '').trim();
		alerts.alert({
			type: 'info',
			alert_id: 'meilisearch-ollama-probe',
			message: '[[meilisearch:admin.ollamaTesting]]',
			timeout: 6000,
		});
		try {
			const response = await api.post('/plugins/meilisearch/test-ollama', { url });
			if (response && response.reachable) {
				alerts.alert({
					type: 'success',
					alert_id: 'meilisearch-ollama-reachable',
				message: response.version
					? `[[meilisearch:admin.ollamaReachable, ${response.version}]]`
					: '[[meilisearch:admin.ollamaReachableNoVersion]]',
					timeout: 3000,
				});
				// Probe passed — continue with cost-confirm if applicable, else save directly
				if (semanticCostChangeDetected()) {
					modals.confirm('[[meilisearch:admin.confirmSemanticSave]]', (confirm) => {
						if (confirm) doSaveSettings();
					});
					return;
				}
				doSaveSettings();
				return;
			}
			// Unreachable — show confirm modal with override option.
			// modals.confirm(message, callback): callback(true) = OK/Save anyway,
			// callback(false) = Cancel/abort. Use translator.translateKey + args as an ARRAY
			// to substitute %1, %2 — bypasses NodeBB's `[[key, arg1, arg2]]` syntax which
			// mis-splits args when an arg contains a comma (e.g. errorDetail
			// "Network error: foo, bar"). translator.compile is NOT a translator — it only
			// BUILDS [[...]] tokens (and would also work here via its escapeArg comma-escaping,
			// but translateKey is more direct).
			const probeUrl = (response && response.probeUrl) || (url || 'http://localhost:11434/api/embeddings');
			const errorDetail = (response && response.error) || 'Unknown error';
			const message = await translator.translateKey(
				'meilisearch:admin.ollamaUnreachableBody',
				[probeUrl, errorDetail],
				translator.getLanguage(),
			);
			modals.confirm(message, (confirm) => {
				if (!confirm) return;  // admin cancelled — save aborted, form preserved
				// Admin chose "Save anyway" — continue with cost-confirm if applicable
				if (semanticCostChangeDetected()) {
					modals.confirm('[[meilisearch:admin.confirmSemanticSave]]', (costConfirm) => {
						if (costConfirm) doSaveSettings();
					});
					return;
				}
				doSaveSettings();
			});
		} catch (err) {
			// NodeBB's own API route unreachable (shouldn't normally happen — server down?)
			// Fall through to save so admin isn't blocked by an unrelated server error
			alerts.error(err.message || 'Ollama probe failed unexpectedly');
			if (semanticCostChangeDetected()) {
				modals.confirm('[[meilisearch:admin.confirmSemanticSave]]', (confirm) => {
					if (confirm) doSaveSettings();
				});
				return;
			}
			doSaveSettings();
		}
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
