-- Create a sample of your data
CREATE OR REPLACE TEMPORARY TABLE sample_data AS
SELECT *
FROM your_table SAMPLE (1%); -- Adjust the sample size as needed

-- Count the number of times each row in the sample appears in query logs
WITH sample_query_counts AS (
  SELECT
    sd.*,
    COUNT(q.query_id) AS query_count
  FROM
    sample_data sd
    LEFT JOIN (SELECT DISTINCT query_id, table_name FROM INFORMATION_SCHEMA.QUERY_HISTORY) q
    ON sd.your_primary_key_column = q.table_name
  GROUP BY
    sd.your_primary_key_column, sd.column1, sd.column2, ... -- List all columns
)

-- Get the query counts and corresponding rows from the sample
SELECT
  sqc.*,
  IFNULL(query_count, 0) AS query_count
FROM
  sample_data sd
  LEFT JOIN sample_query_counts sqc
  ON sd.your_primary_key_column = sqc.your_primary_key_column; -- Match by primary key or unique identifier

-- Optionally, you can filter and sort the results
-- to focus on rows with low query counts
WHERE query_count < 10 -- Adjust the threshold as needed
ORDER BY query_count;