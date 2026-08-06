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

	function onReindex(data) {
		if (!data || !data.topic_progress || !data.post_progress) {
			return;
		}
		const progressBarContainers = document.querySelectorAll('.reindex-progress-container');
		const hasProgress = data.running ||
			Number(data.topic_progress.total) > 0 || Number(data.post_progress.total) > 0;
		if (data.running || hasProgress) {
			progressBarContainers.forEach((container) => {
				container.classList.remove('hidden');
			});
			const topicProgressBar = document.getElementById('topic-reindex-progress');
			setProgress(topicProgressBar, data.topic_progress.current, data.topic_progress.total);
			const postProgressBar = document.getElementById('post-reindex-progress');
			setProgress(postProgressBar, data.post_progress.current, data.post_progress.total);

			document.getElementById('topic-reindex-progress-text').innerText =
				`${Number(data.topic_progress.current) || 0}/${Number(data.topic_progress.total) || 0}`;
			document.getElementById('post-reindex-progress-text').innerText =
				`${Number(data.post_progress.current) || 0}/${Number(data.post_progress.total) || 0}`;
		} else {
			progressBarContainers.forEach((container) => {
				container.classList.add('hidden');
			});
		}
		if (
			!data.running && data.topic_progress.total === data.topic_progress.current &&
			data.post_progress.total === data.post_progress.current
		) {
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

	ACP.init = function () {
		app.enterRoom('admin/plugins/meilisearch');
		settings.load('meilisearch', $('.meilisearch-settings'), () => {
		});
		$('#save').on('click', saveSettings);
		$('#reindex').on('click', reindex);
		formatLocalTimes();
		socket.removeListener('plugins.meilisearch.reindex', onReindex);
		socket.on('plugins.meilisearch.reindex', onReindex);
	};

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
		settings.save('meilisearch', $('.meilisearch-settings'), () => {
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
		modals.confirm('[[meilisearch:admin.confirmReindex]]', (confirm) => {
			if (!confirm) {
				return;
			}
			const request = !forceReindex
				? api.post('/plugins/meilisearch/reindex')
				: api.del('/plugins/meilisearch/reindex');
			request.then(() => {
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
