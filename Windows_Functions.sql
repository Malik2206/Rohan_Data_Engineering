--Problem =  You need row-level calculations that involves OTHER rows
--Example = Show each sale with the running total

--WITHOUT Windows Functions
SELECT
     s1.sale_date,
     s1.amount,
     SUM(s2.amount) as running_total
FROM sales s1
JOIN sales s2 ON s2.sale_date < s1.sale_date
GROUP BY s1.sale_date, s1.amount
ORDER BY s1.sale_date;

--SLOW: 0(n^2) performance

--WITH window functions
SELECT
    sale_date,
    amount,
    SUM(amount) OVER (ORDER BY sale_date) as running_total
FROM sales
ORDER BY sale_date;
-- FAST 0(n) performance

-- LAG: GET value from previous row
-- LEAD: Get value from NEXT row

SELECT
     sale_date,
     amount,
     LAG(amount, 1) OVER (ORDER BY sale_date) as previous_day,
     LEAD(amount, 1) OVER (ORDER BY sale_date) as next_day,
     amount - LAG(amount, 1) OVER (ORDER BY sale_date) as day_over_day_change
FROM daily_sales
ORDER BY sale_date;
