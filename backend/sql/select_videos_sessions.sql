SELECT video_id, view_count, like_count, dislike_count, comment_count, collected_at
FROM videos_sessions
WHERE video_id = {{ video_id }};