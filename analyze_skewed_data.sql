/*
Written by: Susantha Bathige (http://www.sqlservertorque.net/)
Date: 5/16/2017
Purpose: To analyse skewed data distribution of a table
Version: Still needs to be tested for various scenarios in prod environments. 
Usage: Use it with care for very large tables in prod environments. Make sure you've got index coverage
       for the statistics being analysed. 
*/

DROP PROC IF EXISTS dbo.analyse_skewed_data
GO
CREATE PROC dbo.analyse_skewed_data
(
	@ObjectName sysname,
	@StatsID int
)
AS
BEGIN

	SELECT  a.step_number, 
			a.range_high_key AS range_high_key_from, 
			b.range_high_key range_high_key_to, 
			b.range_rows, 
			b.distinct_range_rows, 
			b.average_range_rows,
			x.mean,
			[stdev(SD)]=x.[stdev],
			[variation(CV)]=ROUND(x.[stdev]/(x.mean*1.0),2)*100,
			x.min_row_cnt,
			x.max_row_cnt
	FROM sys.dm_db_stats_histogram (OBJECT_ID(@ObjectName),@StatsID) a
	INNER JOIN sys.dm_db_stats_histogram (OBJECT_ID(@ObjectName),@StatsID) b
		ON a.step_number+1=b.step_number
	CROSS APPLY 
	  ( 
		  SELECT AVG(row_cnt) AS mean, 
				 MIN(row_cnt) AS min_row_cnt, 
				 MAX(row_cnt) AS max_row_cnt, 
				 ROUND(STDEV(row_cnt),2) AS [stdev]
		  FROM 
		  (
			  SELECT  id_1, COUNT(*) AS row_cnt FROM dbo.bigtable
			  WHERE id_1 > CAST(a.range_high_key AS int) AND id_1 < CAST(b.range_high_key AS int)
			  GROUP BY id_1
		  ) y
	  ) x
	ORDER BY [variation(CV)] DESC
END