WITH A AS (
    SELECT 
        video_id,
        view_count,
        like_count,
        dislike_count,
        comment_count,
        collected_at,
        view_count - LAG(view_count, 1) OVER (PARTITION BY video_id ORDER BY collected_at) AS view_count_difference,
        -- 조회수 증가 비율 계산 테스트
        CASE 
            WHEN LAG(view_count, 1) OVER (PARTITION BY video_id ORDER BY collected_at) IS NULL 
            THEN NULL
            ELSE (view_count - LAG(view_count, 1) OVER (PARTITION BY video_id ORDER BY collected_at)) / (LAG(view_count, 1) OVER (PARTITION BY video_id ORDER BY collected_at) * 100)
        END AS view_count_growth_rate
    FROM 
        videos_sessions
    ORDER BY 
        video_id, collected_at
)
SELECT 
    A.video_id AS video_id, 
    A.view_count AS view_count, 
    A.like_count AS like_count, 
    A.dislike_count AS dislike_count, 
    A.comment_count AS comment_count, 
    A.collected_at AS collected_at, 
    A.view_count_difference AS view_count_difference,
    A.view_count_growth_rate AS view_count_growth_rate
FROM 
    A
WHERE 
    A.view_count_growth_rate IS NOT NULL AND A.view_count_difference IS NOT NULL;
