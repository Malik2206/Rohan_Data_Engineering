--Recursive CTE


WITH RECURSIVE cte_name AS (
cte_query_definition) (the anchor member)

UNION ALL

cte_query_definition (the recursive member)
)

SELECT *
b
FROM cte_name

--Example = Generate date series
WITH RECURSIVE date_series AS (
-- Base case: Starting date
SELECT DATE '2024-01-01' as date

UNION ALL

--Recursive case: add 1 day until we hit end date
SELECT date + INTERVAL '1day'
FROM date_series
WHERE date < DATE '2024-01-31'
)

SELECT date
FROM date_series;


