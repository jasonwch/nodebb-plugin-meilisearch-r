<div class="d-flex flex-column">
	{{{each messages}}}
	<div component="chat/search/result" class="rounded-1 search-result" data-roomid="{messages.roomId}">
		<div class="d-flex gap-1 justify-content-between">
			<a href="{messages.chatLink}" onclick="ajaxify.go('{messages.chatLink}'); return false;" class="chat-room-btn position-relative d-flex flex-grow-1 gap-2 justify-content-start align-items-start btn btn-ghost btn-sm ff-sans text-start" style="padding: 0.5rem;">

				<div class="main-avatar">
					{{{ if messages.mainParticipant }}}
					{{buildAvatar(messages.mainParticipant, "32px", true)}}
					{{{ else }}}
					<span class="avatar avatar-rounded" style="--avatar-size: 32px; width:32px; height:32px; background-color: var(--bs-tertiary-bg)">?</span>
					{{{ end }}}
				</div>

				<div class="d-flex flex-grow-1 flex-column w-100" style="min-width:0;">
					<div component="chat/room/title" class="room-name fw-semibold text-xs text-break">
						{messages.roomName}
					</div>
					<div component="chat/room/teaser">

						<div class="teaser-content text-sm line-clamp-3 text-break mb-0">
							{{buildAvatar(messages.user, "14px", true, "align-middle")}}
							<strong class="text-xs fw-semibold teaser-username">{messages.senderName}:</strong>
							{messages.cleanedContent}
						</div>

						<div class="teaser-timestamp text-muted text-xs" style="margin-top: 2px; line-height: 1;">
							<span class="timeago" title="{messages.isoTime}"></span>
						</div>

					</div>
				</div>
			</a>
		</div>
	</div>
	<hr class="my-1">
	{{{end}}}
</div>
