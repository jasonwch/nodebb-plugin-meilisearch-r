'use strict';

const winston = nodebb.require('winston');
const routeHelpers = nodebb.require('./src/routes/helpers');
const settings = nodebb.require('./src/meta/settings');

// Plugin lifecycle + HTTP surface: static:app.load init, the reindex REST API,
// the ACP nav entry, and the admin-notices health line.
module.exports = function attachRoutes(plugin) {
	plugin.init = async function (params) {
		const { router } = params;
		winston.debug('[plugin/meilisearch] Initializing MeiliSearch plugin');
		routeHelpers.setupAdminPageRoute(router, '/admin/plugins/meilisearch', [], async (req, res) => {
			const topicTotal = Number(plugin.indexing.topic_progress.total) || 0;
			const topicCurrent = Number(plugin.indexing.topic_progress.current) || 0;
			const postTotal = Number(plugin.indexing.post_progress.total) || 0;
			const postCurrent = Number(plugin.indexing.post_progress.current) || 0;
			const messageTotal = Number(plugin.indexing.message_progress.total) || 0;
			const messageCurrent = Number(plugin.indexing.message_progress.current) || 0;
			const topicPercent = topicTotal > 0 ? Math.min(100, Math.max(0, Math.round(100 * topicCurrent / topicTotal))) : 0;
			const postPercent = postTotal > 0 ? Math.min(100, Math.max(0, Math.round(100 * postCurrent / postTotal))) : 0;
			const messagePercent = messageTotal > 0 ? Math.min(100, Math.max(0, Math.round(100 * messageCurrent / messageTotal))) : 0;
			const lastReindexResult = await settings.getOne(plugin.id, 'lastReindexResult') || {};
			res.render('admin/plugins/meilisearch', {
				title: '[[meilisearch:admin.settings]]',
				indexing: plugin.indexing,
				topicPercent,
				postPercent,
				messagePercent,
				lastReindexResult,
			});
		});
		await settings.setOnEmpty(plugin.id, plugin.defaults);
		await plugin.prepareSearch();
		await plugin.ensureChatMessageIndex();
		plugin.patchTopicsSearch();
		plugin.initialized = true;
	};

	plugin.addRoutes = async function ({ router, middleware, helpers }) {
		const middlewares = [
			middleware.ensureLoggedIn,
			middleware.admin.checkPrivileges,
		];
		routeHelpers.setupApiRoute(router, 'get', '/meilisearch/reindex', middlewares, async (req, res) => {
			helpers.formatApiResponse(200, res, {
				indexing: plugin.indexing,
				healthy: plugin.healthy,
				lastReindexResult: await settings.getOne(plugin.id, 'lastReindexResult') || {},
			});
		});

		routeHelpers.setupApiRoute(router, 'post', '/meilisearch/reindex', middlewares, async (req, res) => {
			await settings.set(plugin.id, { indexed: false }, true);
			plugin.reindex(false);
			helpers.formatApiResponse(202, res, plugin.indexing);
		});
		routeHelpers.setupApiRoute(router, 'delete', '/meilisearch/reindex', middlewares, async (req, res) => {
			await settings.set(plugin.id, { indexed: false }, true);
			plugin.reindex(true);
			helpers.formatApiResponse(202, res, plugin.indexing);
		});

		// Pure A: Ollama reachability probe. NodeBB container makes a 5s HTTP GET to
		// <url>/api/version (or http://localhost:11434/api/version if URL is blank — the
		// lib/embedder.js default). Used by static/lib/admin.js saveSettings to show a
		// modal when Ollama is unreachable from NodeBB. Test runs from NodeBB's container,
		// not Meilisearch's — admin is informed of this in the modal text so they can
		// override with "Save anyway" if they're confident Meilisearch can reach Ollama
		// on a different network path (e.g. Meilisearch runs with --network=host).
		routeHelpers.setupApiRoute(router, 'post', '/meilisearch/test-ollama', middlewares, async (req, res) => {
			const rawUrl = (req.body && req.body.url) || '';
			// Default URL mirrors lib/embedder.js ollama fallback
			const ollamaUrl = rawUrl || 'http://localhost:11434/api/embeddings';
			// Strip /api/embeddings suffix if admin included it, then append /api/version probe path
			const ollamaBase = ollamaUrl.replace(/\/api\/embeddings\/?$/, '').replace(/\/$/, '');
			const probeUrl = `${ollamaBase}/api/version`;
			// Declare controller/timeout above the try block so the finally clause can always
			// clear the timer — even if the synchronous URL parsing or fetch setup throws
			// before the await resolves (e.g. malformed URL → `new URL()` throws inside fetch).
			const controller = new AbortController();
			const timeout = setTimeout(() => controller.abort(), 5000);
			try {
				const response = await fetch(probeUrl, { signal: controller.signal });
				if (!response.ok) {
					return helpers.formatApiResponse(200, res, {
						reachable: false,
						error: `Ollama responded HTTP ${response.status} ${response.statusText}`,
						probeUrl,
					});
				}
				const data = await response.json().catch(() => ({}));
				return helpers.formatApiResponse(200, res, {
					reachable: true,
					version: data.version,
					probeUrl,
				});
			} catch (err) {
				return helpers.formatApiResponse(200, res, {
					reachable: false,
					error: err.name === 'AbortError' ? 'Timeout after 5s' : err.message,
					probeUrl,
				});
			} finally {
				clearTimeout(timeout);
			}
		});
	};

	plugin.addAdminNavigation = function (header) {
		header.plugins.push({
			route: '/plugins/meilisearch',
			icon: 'fa-tint',
			name: 'Meilisearch',
		});

		return header;
	};

	plugin.getNotices = async function (notices) {
		const checkHealth = await plugin.checkHealth();
		notices.push({
			done: checkHealth,
			doneText: 'MeiliSearch connection OK',
			notDoneText: 'Could not connect to MeiliSearch',
		});
		return notices;
	};
};
