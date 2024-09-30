SELECT id, categoryId, publishedAt, channelId, title, description
FROM videos
WHERE id = {{ video_id }};