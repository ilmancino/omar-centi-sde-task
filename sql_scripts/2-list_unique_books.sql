WITH counts
     AS (SELECT list_id,
                Count(DISTINCT book_sk) AS unique_books
         FROM   fact_publications
         GROUP  BY list_id),
     ranked
     AS (SELECT list_id,
                unique_books,
                Dense_rank()
                  over(
                    ORDER BY unique_books ASC) AS ranking
         FROM   counts)
SELECT l.list_name,
       r.unique_books
FROM   ranked r
       inner join dim_lists l
               ON r.list_id = l.list_id
WHERE  ranking <= 3
ORDER  BY 2 ASC 
