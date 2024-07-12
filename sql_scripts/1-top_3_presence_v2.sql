WITH top_3
     AS (SELECT book_sk,
                published_date
         FROM   fact_publications fp
         WHERE  published_date BETWEEN %(start_date)s AND %(end_date)s
                AND rank BETWEEN 1 AND 3),
     grouped
     AS (SELECT book_sk,
                Count(*) AS appeareances
         FROM   top_3
         GROUP  BY book_sk),
     ranking
     AS (SELECT book_sk,
                appeareances,
                Rank()
                  over(
                    ORDER BY appeareances DESC) AS ranking
         FROM   grouped)
SELECT title,
       author,
       primary_isbn10,
       primary_isbn13,
       appeareances
FROM   ranking r
       inner join dim_books db
               ON r.book_sk = db.book_sk
WHERE  ranking = 1 
