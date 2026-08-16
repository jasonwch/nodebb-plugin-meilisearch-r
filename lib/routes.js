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
