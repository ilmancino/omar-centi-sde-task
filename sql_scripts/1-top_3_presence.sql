--this version gets the book which remained the longest, in consecutive, 
--publications in the top 3
WITH top_3_ranked
     AS (
        -- assign an incremental id to publications based on date
        -- also getting only top 3 books in each publication
        SELECT DISTINCT published_date,
                        book_sk,
                        Dense_rank()
                          over(
                            ORDER BY published_date ASC) AS publication_id
         FROM   fact_publications f
         WHERE  published_date BETWEEN %(start_date)s AND %(end_date)s
                AND rank BETWEEN 1 AND 3),
     flagged
     AS (
        -- assign a flag to identify if a book is appearing in a consecutive publication
        SELECT *,
               -- in the subtraction below, 1 means is consecutive
               publication_id - Lag(publication_id)
                                  over(
                                    PARTITION BY book_sk
                                    ORDER BY published_date ASC) AS flag
         FROM   top_3_ranked),
     global_n_group_index
     AS (
        -- add incremental id for consecutive/no consecutive appearance of a book
        SELECT *,
               Row_number()
                 over(
                   PARTITION BY book_sk, flag
                   ORDER BY published_date ASC) AS group_index
         FROM   flagged),
     sub_groups
     AS (
        -- add identifier of consecutive/non consecutive sub groups
        SELECT *,
               publication_id - group_index AS sub_group
         FROM   global_n_group_index),
     consecutive_top
     AS (
        -- counts consecutive appearences, sub_group helps identifying correctly interruptions in appearences 
        SELECT book_sk,
               flag,
               sub_group,
               Count(*) + 1 AS qty
         FROM   sub_groups
         WHERE  flag = 1
         GROUP  BY book_sk, flag, sub_group
         ORDER  BY book_sk, flag, sub_group),
     consecutives_ranked
     AS (
        -- just ranking the books by most appearences
        SELECT book_sk,
               qty,
               Rank()
                 over(
                   ORDER BY qty DESC) AS consecutives_rank
         FROM   consecutive_top)
SELECT db.title,
       db.author,
       db.primary_isbn10,
       db.primary_isbn13,
       cr.qty AS consecutive_publications_on_top3
FROM   consecutives_ranked cr
       inner join dim_books db
               ON cr.book_sk = db.book_sk
WHERE  consecutives_rank = 1 
