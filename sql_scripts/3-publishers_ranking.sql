WITH grouped
     AS (SELECT Extract(year FROM published_date)    AS published_year,
                Extract(quarter FROM published_date) AS published_quarter,
                publisher,
                SUM(CASE
                      WHEN rank > 0 THEN 6 - rank
                      ELSE 0
                    END)                             AS points
         FROM   fact_publications
         WHERE  published_date BETWEEN %(start_date)s AND %(end_date)s
         GROUP  BY 1, 2, 3),
     ranking
     AS (SELECT published_year,
                published_quarter,
                publisher,
                points,
                Dense_rank()
                  over(
                    PARTITION BY published_year, published_quarter
                    ORDER BY points DESC) AS ranking
         FROM   grouped)
SELECT Cast(published_year AS INTEGER) AS published_year,
       'Q' || published_quarter as published_quarter,
       publisher,
       points,
       ranking
FROM   ranking
WHERE  ranking <= 5
ORDER  BY 1, 2, 5 
