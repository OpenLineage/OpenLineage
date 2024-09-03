WITH ns AS (
    SELECT author_id, author_name
    FROM authors
    WHERE author_id <= 20
)
SELECT ns.*
FROM ns