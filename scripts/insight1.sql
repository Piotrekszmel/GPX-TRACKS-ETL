WITH fast_tracks AS (
    SELECT MAX(speed) as max_speed
    FROM gpx_tracks
    GROUP BY track_id
    HAVING max_speed > 10
)

SELECT COUNT(*) * 100 / (SELECT COUNT(DISTINCT track_id) FROM gpx_tracks)
FROM fast_tracks