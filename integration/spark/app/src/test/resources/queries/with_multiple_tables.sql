WITH ns AS (
    SELECT author_id AS id
    FROM books
    WHERE book_id <= 20
)
SELECT authors.*
FROM authors
INNER JOIN ns ON authors.author_id = ns.id