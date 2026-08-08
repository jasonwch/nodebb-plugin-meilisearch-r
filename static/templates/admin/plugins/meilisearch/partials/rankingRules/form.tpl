<form>
    <div class="form-group">
        <label for="rule">{{tx("meilisearch:admin.rankingRulesFormLabel")}}</label>
        <input type="text" id="rule" name="rule" class="form-control" placeholder="eg. cid:asc" />
        <p class="help-block">{{tx("meilisearch:admin.rankingRulesFormHelp")}} <a href="https://docs.meilisearch.com/learn/core_concepts/relevancy.html#custom-rules" style="word-break: break-all;">https://docs.meilisearch.com/learn/core_concepts/relevancy.html#custom-rules</a></p>
    </div>
</form>
