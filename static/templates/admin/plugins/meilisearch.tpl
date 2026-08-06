<div class="card">
	<div class="card-header">
		<h2 class="card-title">{{tx("meilisearch:admin.actions")}}</h2>
	</div>
	<div class="card-body">
		<div class="row">
			<div class="col-sm-2 col-xs-12 settings-header">{{tx("meilisearch:admin.index")}}</div>
			<div class="col-sm-10 col-xs-12">
				<div class="row">
					<div class="col-sm-{{{ if lastReindexResult.finishedAt }}}8{{{ else }}}12{{{ end }}} col-xs-12">
						<p class="help-block">{{tx("meilisearch:admin.reindexHelp")}}</p>
						<div class="reindex-actions">
							<button type="button" id="reindex" class="btn btn-danger btn-sm">{{tx("meilisearch:admin.reindex")}}</button>
						</div>
						<div class="text-center">
							<div class="checkbox force-reindex-container" id="reindex-force-container">
								<label for="force-reindex" class="mdl-switch mdl-js-switch mdl-js-ripple-effect">
									<input type="checkbox" class="mdl-switch__input" id="force-reindex" name="force-reindex">
									<span class="mdl-switch__label"><strong>{{tx("meilisearch:admin.forceReindex")}}</strong></span>
								</label>
							</div>
						</div>
						<div class="col-sm-12 col-xs-12 reindex-progress-container {{{ if !indexing.running}}}hidden{{{end}}}">
							<h4 class="text-center">{{tx("meilisearch:admin.reindexTopicsProgress")}} <span id="topic-reindex-progress-text">{indexing.topic_progress.current}/{indexing.topic_progress.total}</span></h4>
							<div class="progress">
								<div id="topic-reindex-progress" class="progress-bar reindex-progress-bar" role="progressbar" aria-valuenow="{indexing.topic_progress.current}" aria-valuemin="0" aria-valuemax="{indexing.topic_progress.total}" style="width: {{{ if topicPercent }}}{topicPercent}%{{{ else }}}0%{{{ end }}};">
									{indexing.topic_progress.current}/{indexing.topic_progress.total}
								</div>
							</div>
						</div>
						<div class="col-sm-12 col-xs-12 reindex-progress-container {{{ if !indexing.running}}}hidden{{{end}}}">
							<h4 class="text-center">{{tx("meilisearch:admin.reindexPostsProgress")}} <span id="post-reindex-progress-text">{indexing.post_progress.current}/{indexing.post_progress.total}</span></h4>
							<div class="progress">
								<div id="post-reindex-progress" class="progress-bar reindex-progress-bar" role="progressbar" aria-valuenow="{indexing.post_progress.current}" aria-valuemin="0" aria-valuemax="{indexing.post_progress.total}" style="width: {{{ if postPercent }}}{postPercent}%{{{ else }}}0%{{{ end }}};">
									{indexing.post_progress.current}/{indexing.post_progress.total}
								</div>
							</div>
						</div>
					</div>
					{{{ if lastReindexResult.finishedAt }}}
					<div class="col-sm-4 col-xs-12">
						<h4 class="text-center last-reindex-title">{{tx("meilisearch:admin.lastReindex")}}</h4>
						<div class="table-responsive">
						<table class="table table-bordered table-condensed last-reindex-table">
							<tbody>
								<tr>
									<th>{{tx("meilisearch:admin.lastReindexDate")}}</th>
									<td><span data-finished-at="{lastReindexResult.finishedAt}">&mdash;</span></td>
								</tr>
								<tr>
									<th>{{tx("meilisearch:admin.lastReindexTopics")}}</th>
									<td>{lastReindexResult.topic_progress.current}/{lastReindexResult.topic_progress.total}</td>
								</tr>
							<tr>
								<th>{{tx("meilisearch:admin.lastReindexPosts")}}</th>
								<td>{lastReindexResult.post_progress.current}/{lastReindexResult.post_progress.total}</td>
							</tr>
							{{{ if lastReindexResult.skippedDeletedTopics }}}
							<tr>
								<th>{{tx("meilisearch:admin.lastReindexSkippedDeletedTopics")}}</th>
								<td>{lastReindexResult.skippedDeletedTopics}</td>
							</tr>
							{{{ end }}}
							{{{ if lastReindexResult.skippedDeletedPosts }}}
							<tr>
								<th>{{tx("meilisearch:admin.lastReindexSkippedDeletedPosts")}}</th>
								<td>{lastReindexResult.skippedDeletedPosts}</td>
							</tr>
							{{{ end }}}
								<tr>
									<th>{{tx("meilisearch:admin.lastReindexStatus")}}</th>
									<td>
										{{{ if lastReindexResult.success }}}
										<span class="label label-success">{{tx("meilisearch:admin.lastReindexSuccess")}}</span>
										{{{ else }}}
										<span class="label label-danger">{{tx("meilisearch:admin.lastReindexFailed")}}</span>
										{{{ end }}}
									</td>
								</tr>
								{{{ if lastReindexResult.error }}}
								<tr>
									<th>{{tx("meilisearch:admin.lastReindexError")}}</th>
									<td class="text-danger">{lastReindexResult.error}</td>
								</tr>
								{{{ end }}}
							</tbody>
						</table>
						</div>
					</div>
					{{{ end }}}
				</div>
			</div>
		</div>
	</div>
</div>
<div class="card">
	<div class="card-header">
		<h2 class="card-title">{{tx("meilisearch:admin.settings")}}</h2>
	</div>
	<div class="card-body">
		<form role="form" class="meilisearch-settings">
			<div class="row">
				<div class="col-sm-2 col-xs-12 settings-header">{{tx("meilisearch:admin.connection")}}</div>
				<div class="col-sm-10 col-xs-12">
					<div class="form-group">
						<label for="host">{{tx("meilisearch:admin.host")}}</label>
						<input type="text" id="host" name="host" title="Host" class="form-control" placeholder="http://localhost:7700">
					</div>
					<div class="form-group">
						<label for="apiKey">{{tx("meilisearch:admin.apiKey")}}</label>
						<input type="password" id="apiKey" name="apiKey" title="API Key" class="form-control" placeholder="*****">
					</div>
					<div class="form-group">
						<label for="healthCheckInterval">{{tx("meilisearch:admin.healthCheckInterval")}}</label>
						<input type="number" id="healthCheckInterval" name="healthCheckInterval" title="Health Check Interval" class="form-control" placeholder="60">
					</div>
				</div>
			</div>

			<br />

			<div class="row">
				<div class="col-sm-2 col-xs-12 settings-header">{{tx("meilisearch:admin.search")}}</div>
				<div class="col-sm-10 col-xs-12">
					<div class="form-group">
						<label for="maxDocuments">{{tx("meilisearch:admin.maxDocuments")}}</label>
						<input type="number" id="maxDocuments" name="maxDocuments" title="Max Documents" class="form-control" placeholder="500">
					</div>
					<div class="form-group">
						<label for="searchMinTermLength">{{tx("meilisearch:admin.searchMinTermLength")}}</label>
						<input type="number" id="searchMinTermLength" name="searchMinTermLength" min="2" title="{{tx("meilisearch:admin.searchMinTermLength")}}" class="form-control" placeholder="2">
						<p class="help-block">{{tx("meilisearch:admin.searchMinTermLengthHelp")}}</p>
					</div>
					<div class="form-group" data-type="sorted-list" data-sorted-list="rankingRules" data-item-template="admin/plugins/meilisearch/partials/rankingRules/item" data-form-template="admin/plugins/meilisearch/partials/rankingRules/form">
						<label for="rankingRulesList">{{tx("meilisearch:admin.rankingRules")}}</label>
						<p class="help-block">{{tx("meilisearch:admin.rankingRulesHelp")}} <a href="https://docs.meilisearch.com/learn/core_concepts/relevancy.html#ranking-rules">https://docs.meilisearch.com/learn/core_concepts/relevancy.html#ranking-rules</a></p>
						<ul name="rankingRulesList" data-type="list" class="list-group"></ul>
						<button type="button" data-type="add" class="btn btn-info">{{tx("meilisearch:admin.addRankingRule")}}</button>
					</div>
					<div class="form-group" data-type="sorted-list" data-sorted-list="stopWords" data-item-template="admin/plugins/meilisearch/partials/stopWords/item" data-form-template="admin/plugins/meilisearch/partials/stopWords/form">
						<label for="stopWordsList">{{tx("meilisearch:admin.stopWords")}}</label>
						<p class="help-block">{{tx("meilisearch:admin.stopWordsHelp")}} <a href="https://docs.meilisearch.com/reference/api/settings.html#get-stop-words">https://docs.meilisearch.com/reference/api/settings.html#get-stop-words</a></p>
						<ul name="stopWordsList" data-type="list" class="list-group"></ul>
						<button type="button" data-type="add" class="btn btn-info">{{tx("meilisearch:admin.addStopWord")}}</button>
					</div>
					<div class="form-group" data-type="sorted-list" data-sorted-list="synonyms" data-item-template="admin/plugins/meilisearch/partials/synonyms/item" data-form-template="admin/plugins/meilisearch/partials/synonyms/form">
						<label for="synonymsList">{{tx("meilisearch:admin.synonyms")}}</label>
						<p class="help-block">{{tx("meilisearch:admin.synonymsHelp")}} <a href="https://docs.meilisearch.com/learn/configuration/synonyms.html">https://docs.meilisearch.com/learn/configuration/synonyms.html</a></p>
						<ul name="synonymsList" data-type="list" class="list-group"></ul>
						<button type="button" data-type="add" class="btn btn-info">{{tx("meilisearch:admin.addSynonym")}}</button>
					</div>
				</div>
			</div>
			<div class="row">
				<div class="col-sm-2 col-xs-12 settings-header">{{tx("meilisearch:admin.typoTolerance")}}</div>
				<div class="col-sm-10 col-xs-12">
					<div class="checkbox" id="typoTolerance-container">
						<label for="typoTolerance" class="mdl-switch mdl-js-switch mdl-js-ripple-effect">
							<input type="checkbox" class="mdl-switch__input" id="typoTolerance" name="typoTolerance">
							<span class="mdl-switch__label"><strong>{{tx("meilisearch:admin.typoToleranceEnable")}}</strong></span>
						</label>
					</div>
					<div class="form-group">
						<label for="typoToleranceMinWordSizeOneTypo">{{tx("meilisearch:admin.typoToleranceMinWordSizeOneTypo")}}</label>
						<input type="number" id="typoToleranceMinWordSizeOneTypo" name="typoToleranceMinWordSizeOneTypo" title="Typo Tolerance Min Word Size One Typo" class="form-control" placeholder="5">
					</div>
					<div class="form-group">
						<label for="typoToleranceMinWordSizeTwoTypos">{{tx("meilisearch:admin.typoToleranceMinWordSizeTwoTypos")}}</label>
						<input type="number" id="typoToleranceMinWordSizeTwoTypos" name="typoToleranceMinWordSizeTwoTypos" title="Typo Tolerance Min Word Size Two Typos" class="form-control" placeholder="9">
					</div>
					<div class="form-group" data-type="sorted-list" data-sorted-list="typoToleranceDisableOnWords" data-item-template="admin/plugins/meilisearch/partials/typoToleranceDisableOnWords/item" data-form-template="admin/plugins/meilisearch/partials/typoToleranceDisableOnWords/form">
						<label for="typoToleranceDisableOnWordsList">{{tx("meilisearch:admin.typoToleranceDisableOnWords")}}</label>
						<p class="help-block">{{tx("meilisearch:admin.typoToleranceDisableOnWordsHelp")}} <a href="https://docs.meilisearch.com/learn/configuration/typo_tolerance.html#disableonwords">https://docs.meilisearch.com/learn/configuration/typo_tolerance.html#disableonwords</a></p>
						<ul name="typoToleranceDisableOnWordsList" data-type="list" class="list-group"></ul>
						<button type="button" data-type="add" class="btn btn-info">{{tx("meilisearch:admin.addTypoToleranceDisabledWord")}}</button>
					</div>
				</div>
			</div>
		</form>
		<button id="save" class="floating-button mdl-button mdl-js-button mdl-button--fab mdl-js-ripple-effect mdl-button--colored">
			<i class="material-icons">{{tx("meilisearch:admin.save")}}</i>
		</button>
	</div>
</div>
