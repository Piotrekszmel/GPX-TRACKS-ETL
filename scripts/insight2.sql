WITH cte AS (
  SELECT origin_city, dest_city
  FROM gpx_tracks
  WHERE origin_city = 'Cowes' OR dest_city = 'Cowes'
)
SELECT city, SUM(percentage)
FROM (
  SELECT origin_city as city,
    CASE 
      WHEN origin_city = 'Cowes' AND dest_city <> 'Cowes' THEN 0
      ELSE COUNT(*) * 100.0 / (SELECT COUNT(*) FROM gpx_tracks)
    END AS percentage
  FROM cte
  GROUP BY origin_city, dest_city
  UNION
  SELECT dest_city AS city, 
  	CASE 
      WHEN dest_city = 'Cowes' AND origin_city <> 'Cowes' THEN 0
      ELSE COUNT(*) * 100.0 / (SELECT COUNT(*) FROM gpx_tracks)
    END AS percentage
  FROM cte
  GROUP BY dest_city, origin_city
) subquery
GROUP BY city;	
