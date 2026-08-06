<form>
    <div class="form-group">
        <label for="word">{{tx("meilisearch:admin.synonymsWordFormLabel")}}</label>
        <input type="text" id="word" name="word" class="form-control" placeholder="word" />
        <label for="synonyms">{{tx("meilisearch:admin.synonymsSynonymsFormLabel")}}</label>
        <input type="text" id="synonyms" name="synonyms" class="form-control" placeholder="term,utterance" />
        <p class="help-block">{{tx("meilisearch:admin.synonymsFormHelp")}}</p>
    </div>
</form>
