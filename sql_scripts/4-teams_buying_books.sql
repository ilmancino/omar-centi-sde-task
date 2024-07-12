WITH first_times
     AS (SELECT book_sk,
                Min(CASE
                      WHEN rank = 1 THEN published_date
                      ELSE NULL
                    END) AS first_ranked_1st,
                Min(CASE
                      WHEN rank = 3 THEN published_date
                      ELSE NULL
                    END) AS first_ranked_3rd
         FROM   fact_publications fb
         WHERE  published_date BETWEEN %(start_date)s AND %(end_date)s
                AND rank IN ( 1, 3 )
         GROUP  BY book_sk)
SELECT db.title,
       db.author,
       db.primary_isbn10,
       db.primary_isbn13,
       CASE
         WHEN Coalesce(first_ranked_1st, '2099-12-31') <
              Coalesce(first_ranked_3rd, '2099-12-31') THEN 'Jakes_team'
         ELSE 'Petes_team'
       END AS bought_by
FROM   first_times ft
       inner join dim_books db
               ON ft.book_sk = db.book_sk
ORDER  BY 1 
