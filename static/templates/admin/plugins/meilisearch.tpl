<div class="acp-page-container">
	<!-- IMPORT admin/partials/settings/header.tpl -->

	<div class="row m-0">
		<div id="spy-container" class="col-12 col-md-8 px-0 mb-4" tabindex="0">

			<!-- 1. Index result -->
			<div id="index-result" class="mb-4">
				<h5 class="fw-bold tracking-tight settings-header">{{tx("meilisearch:admin.indexResult")}}</h5>
				<div class="card text-xs">
					<div class="card-header d-flex align-items-center justify-content-between">
						<span class="fw-bold">{{tx("meilisearch:admin.lastReindex")}}</span>
						{{{ if !lastReindexResult.finishedAt }}}
						<span class="badge bg-secondary">{{tx("meilisearch:admin.lastReindexNever")}}</span>
						{{{ else }}}
							{{{ if lastReindexResult.success }}}
							<span class="badge bg-success">{{tx("meilisearch:admin.lastReindexSuccess")}}</span>
							{{{ else }}}
							<span class="badge bg-danger">{{tx("meilisearch:admin.lastReindexFailed")}}</span>
							{{{ end }}}
						{{{ end }}}
					</div>
					<div class="card-body">
						{{{ if lastReindexResult.finishedAt }}}
						<p class="mb-3"><strong>{{tx("meilisearch:admin.lastReindexDate")}}:</strong> <span data-finished-at="{lastReindexResult.finishedAt}">&mdash;</span></p>
						<div class="row g-3 mb-3">
							<div class="col-6">
								<strong>{{tx("meilisearch:admin.lastReindexTopics")}}</strong><br>
								<span>{{lastReindexResult.topic_progress.current}}/{{lastReindexResult.topic_progress.total}}</span>
							</div>
							<div class="col-6">
								<strong>{{tx("meilisearch:admin.lastReindexPosts")}}</strong><br>
								<span>{{lastReindexResult.post_progress.current}}/{{lastReindexResult.post_progress.total}}</span>
							</div>
							<div class="col-6">
								<strong>{{tx("meilisearch:admin.lastReindexMessages")}}</strong><br>
								<span>{{lastReindexResult.message_progress.current}}/{{lastReindexResult.message_progress.total}}</span>
							</div>
							{{{ if lastReindexResult.skippedSystemMessages }}}
							<div class="col-6">
								<strong>{{tx("meilisearch:admin.lastReindexSkippedSystemMessages")}}</strong><br>
								<span>{{lastReindexResult.skippedSystemMessages}}</span>
							</div>
							{{{ end }}}
							{{{ if lastReindexResult.skippedDeletedTopics }}}
							<div class="col-6">
								<strong>{{tx("meilisearch:admin.lastReindexSkippedDeletedTopics")}}</strong><br>
								<span>{{lastReindexResult.skippedDeletedTopics}}</span>
							</div>
							{{{ end }}}
							{{{ if lastReindexResult.skippedDeletedPosts }}}
							<div class="col-6">
								<strong>{{tx("meilisearch:admin.lastReindexSkippedDeletedPosts")}}</strong><br>
								<span>{{lastReindexResult.skippedDeletedPosts}}</span>
							</div>
							{{{ end }}}
							{{{ if lastReindexResult.skippedDeletedMessages }}}
							<div class="col-6">
								<strong>{{tx("meilisearch:admin.lastReindexSkippedDeletedMessages")}}</strong><br>
								<span>{{lastReindexResult.skippedDeletedMessages}}</span>
							</div>
							{{{ end }}}
							{{{ if lastReindexResult.skippedOrphanMessages }}}
							<div class="col-6">
								<strong>{{tx("meilisearch:admin.lastReindexSkippedOrphanMessages")}}</strong><br>
								<span>{{lastReindexResult.skippedOrphanMessages}}</span>
							</div>
							{{{ end }}}
						</div>

						{{{ if lastReindexResult.error }}}
						<div class="text-danger">
							<strong>{{tx("meilisearch:admin.lastReindexError")}}:</strong> {lastReindexResult.error}
						</div>
						{{{ end }}}
						{{{ else }}}
						<p class="text-muted mb-3">{{tx("meilisearch:admin.lastReindexNever")}}</p>
						{{{ end }}}
					</div>
				</div>
			</div>

			<!-- 2. Index action -->
			<div id="index-action" class="mb-4">
				<h5 class="fw-bold tracking-tight settings-header">{{tx("meilisearch:admin.indexAction")}}</h5>
				<p class="form-text">{{tx("meilisearch:admin.reindexHelp")}}</p>
				<div class="reindex-actions mb-3">
					<button type="button" id="reindex" class="btn btn-danger btn-sm">{{tx("meilisearch:admin.reindex")}}</button>
				</div>
				<div class="d-flex justify-content-center align-items-center mb-3 force-reindex-container" id="reindex-force-container">
					<div class="form-check form-switch mb-0">
						<input class="form-check-input" type="checkbox" id="force-reindex" name="force-reindex">
						<label class="form-check-label" for="force-reindex"><strong>{{tx("meilisearch:admin.forceReindex")}}</strong></label>
					</div>
				</div>
				<div class="reindex-progress-container {{{ if !indexing.running}}}hidden{{{end}}}">
					<h6 class="text-center">{{tx("meilisearch:admin.reindexTopicsProgress")}} <span id="topic-reindex-progress-text">{indexing.topic_progress.current}/{indexing.topic_progress.total}</span></h6>
					<div class="progress mb-3">
						<div id="topic-reindex-progress" class="progress-bar reindex-progress-bar" role="progressbar" aria-valuenow="{indexing.topic_progress.current}" aria-valuemin="0" aria-valuemax="{indexing.topic_progress.total}" style="width: {{{ if topicPercent }}}{topicPercent}%{{{ else }}}0%{{{ end }}};">
							{indexing.topic_progress.current}/{indexing.topic_progress.total}
						</div>
					</div>
					<h6 class="text-center">{{tx("meilisearch:admin.reindexPostsProgress")}} <span id="post-reindex-progress-text">{indexing.post_progress.current}/{indexing.post_progress.total}</span></h6>
					<div class="progress mb-3">
						<div id="post-reindex-progress" class="progress-bar reindex-progress-bar" role="progressbar" aria-valuenow="{indexing.post_progress.current}" aria-valuemin="0" aria-valuemax="{indexing.post_progress.total}" style="width: {{{ if postPercent }}}{postPercent}%{{{ else }}}0%{{{ end }}};">
							{indexing.post_progress.current}/{indexing.post_progress.total}
						</div>
					</div>
					<h6 class="text-center">{{tx("meilisearch:admin.reindexMessagesProgress")}} <span id="message-reindex-progress-text">{indexing.message_progress.current}/{indexing.message_progress.total}</span></h6>
					<div class="progress mb-3">
						<div id="message-reindex-progress" class="progress-bar reindex-progress-bar" role="progressbar" aria-valuenow="{indexing.message_progress.current}" aria-valuemin="0" aria-valuemax="{indexing.message_progress.total}" style="width: {{{ if messagePercent }}}{messagePercent}%{{{ else }}}0%{{{ end }}};">
							{indexing.message_progress.current}/{indexing.message_progress.total}
						</div>
					</div>
				</div>
			</div>

			<!-- Settings form -->
			<form role="form" class="meilisearch-settings">

				<!-- 3. Connection settings -->
				<div id="connection-settings" class="mb-4">
					<h5 class="fw-bold tracking-tight settings-header">{{tx("meilisearch:admin.connection")}}</h5>
					<div class="mb-3">
						<label class="form-label" for="host">{{tx("meilisearch:admin.host")}}</label>
						<input type="text" id="host" name="host" title="Host" class="form-control" placeholder="http://localhost:7700">
					</div>
					<div class="mb-3">
						<label class="form-label" for="apiKey">{{tx("meilisearch:admin.apiKey")}}</label>
						<input type="password" id="apiKey" name="apiKey" title="API Key" class="form-control" placeholder="*****">
					</div>
					<div class="mb-3">
						<label class="form-label" for="healthCheckInterval">{{tx("meilisearch:admin.healthCheckInterval")}}</label>
						<input type="number" id="healthCheckInterval" name="healthCheckInterval" title="Health Check Interval" class="form-control" placeholder="60">
					</div>
				</div>

				<!-- 4. Search options -->
				<div id="search-options" class="mb-4">
					<h5 class="fw-bold tracking-tight settings-header">{{tx("meilisearch:admin.search")}}</h5>
					<div class="mb-3">
						<label class="form-label" for="maxDocuments">{{tx("meilisearch:admin.maxDocuments")}}</label>
						<input type="number" id="maxDocuments" name="maxDocuments" title="Max Documents" class="form-control" placeholder="500">
						<p class="form-text">
							<span class="text-danger fw-bold fst-italic">({{tx("meilisearch:admin.perRoomSearchLimit")}})</span>
						</p>
					</div>
				<div class="mb-3">
					<div class="form-check form-switch">
						<input class="form-check-input" type="checkbox" id="globalChatSearchEnabled" name="globalChatSearchEnabled">
						<label class="form-check-label" for="globalChatSearchEnabled"><strong>{{tx("meilisearch:admin.globalChatSearchEnabled")}}</strong></label>
					</div>
					<p class="form-text" id="globalChatSearchLimitNotice" style="display:none;">
						<span class="text-danger fw-bold fst-italic">({{tx("meilisearch:admin.globalChatSearchLimit")}})</span>
					</p>
				</div>
					<div class="mb-3">
						<label class="form-label" for="searchMinTermLength">{{tx("meilisearch:admin.searchMinTermLength")}}</label>
						<input type="number" id="searchMinTermLength" name="searchMinTermLength" min="2" title="{{tx("meilisearch:admin.searchMinTermLength")}}" class="form-control" placeholder="2">
						<p class="form-text">{{tx("meilisearch:admin.searchMinTermLengthHelp")}}</p>
					</div>
				</div>

				<!-- 5. Ranking rules -->
				<div id="ranking-rules" class="mb-4">
					<h5 class="fw-bold tracking-tight settings-header">{{tx("meilisearch:admin.rankingRules")}}</h5>
					<p class="form-text">{{tx("meilisearch:admin.rankingRulesHelp")}} <a href="https://docs.meilisearch.com/learn/core_concepts/relevancy.html#ranking-rules" style="word-break: break-all;">https://docs.meilisearch.com/learn/core_concepts/relevancy.html#ranking-rules</a></p>
					<div class="form-group" data-type="sorted-list" data-sorted-list="rankingRules" data-item-template="admin/plugins/meilisearch/partials/rankingRules/item" data-form-template="admin/plugins/meilisearch/partials/rankingRules/form">
						<ul name="rankingRulesList" data-type="list" class="list-group"></ul>
						<button type="button" data-type="add" class="btn btn-info">{{tx("meilisearch:admin.addRankingRule")}}</button>
					</div>
				</div>

				<!-- 6. Stop words -->
				<div id="stop-words" class="mb-4">
					<h5 class="fw-bold tracking-tight settings-header">{{tx("meilisearch:admin.stopWords")}}</h5>
					<p class="form-text">{{tx("meilisearch:admin.stopWordsHelp")}} <a href="https://docs.meilisearch.com/reference/api/settings.html#get-stop-words" style="word-break: break-all;">https://docs.meilisearch.com/reference/api/settings.html#get-stop-words</a></p>
					<div class="form-group" data-type="sorted-list" data-sorted-list="stopWords" data-item-template="admin/plugins/meilisearch/partials/stopWords/item" data-form-template="admin/plugins/meilisearch/partials/stopWords/form">
						<ul name="stopWordsList" data-type="list" class="list-group"></ul>
						<button type="button" data-type="add" class="btn btn-info">{{tx("meilisearch:admin.addStopWord")}}</button>
					</div>
				</div>

				<!-- 7. Synonyms -->
				<div id="synonyms" class="mb-4">
					<h5 class="fw-bold tracking-tight settings-header">{{tx("meilisearch:admin.synonyms")}}</h5>
					<p class="form-text">{{tx("meilisearch:admin.synonymsHelp")}} <a href="https://docs.meilisearch.com/learn/configuration/synonyms.html" style="word-break: break-all;">https://docs.meilisearch.com/learn/configuration/synonyms.html</a></p>
					<div class="form-group" data-type="sorted-list" data-sorted-list="synonyms" data-item-template="admin/plugins/meilisearch/partials/synonyms/item" data-form-template="admin/plugins/meilisearch/partials/synonyms/form">
						<ul name="synonymsList" data-type="list" class="list-group"></ul>
						<button type="button" data-type="add" class="btn btn-info">{{tx("meilisearch:admin.addSynonym")}}</button>
					</div>
				</div>

				<!-- 8. Typo tolerance -->
				<div id="typo-tolerance" class="mb-4">
					<h5 class="fw-bold tracking-tight settings-header">{{tx("meilisearch:admin.typoTolerance")}}</h5>
				<div class="form-check form-switch mb-3" id="typoTolerance-container">
					<input class="form-check-input" type="checkbox" id="typoTolerance" name="typoTolerance">
					<label class="form-check-label" for="typoTolerance"><strong>{{tx("meilisearch:admin.typoToleranceEnable")}}</strong></label>
				</div>
					<div class="mb-3">
						<label class="form-label" for="typoToleranceMinWordSizeOneTypo">{{tx("meilisearch:admin.typoToleranceMinWordSizeOneTypo")}}</label>
						<input type="number" id="typoToleranceMinWordSizeOneTypo" name="typoToleranceMinWordSizeOneTypo" title="Typo Tolerance Min Word Size One Typo" class="form-control" placeholder="5">
					</div>
					<div class="mb-3">
						<label class="form-label" for="typoToleranceMinWordSizeTwoTypos">{{tx("meilisearch:admin.typoToleranceMinWordSizeTwoTypos")}}</label>
						<input type="number" id="typoToleranceMinWordSizeTwoTypos" name="typoToleranceMinWordSizeTwoTypos" title="Typo Tolerance Min Word Size Two Typos" class="form-control" placeholder="9">
					</div>
					<p class="form-text">{{tx("meilisearch:admin.typoToleranceDisableOnWordsHelp")}} <a href="https://docs.meilisearch.com/learn/configuration/typo_tolerance.html#disableonwords" style="word-break: break-all;">https://docs.meilisearch.com/learn/configuration/typo_tolerance.html#disableonwords</a></p>
					<div class="form-group" data-type="sorted-list" data-sorted-list="typoToleranceDisableOnWords" data-item-template="admin/plugins/meilisearch/partials/typoToleranceDisableOnWords/item" data-form-template="admin/plugins/meilisearch/partials/typoToleranceDisableOnWords/form">
						<label class="form-label" for="typoToleranceDisableOnWordsList">{{tx("meilisearch:admin.typoToleranceDisableOnWords")}}</label>
						<ul name="typoToleranceDisableOnWordsList" data-type="list" class="list-group"></ul>
						<button type="button" data-type="add" class="btn btn-info">{{tx("meilisearch:admin.addTypoToleranceDisabledWord")}}</button>
					</div>
				</div>

			</form>
		</div>

		<!-- IMPORT admin/partials/settings/toc.tpl -->
	</div>
</div>
