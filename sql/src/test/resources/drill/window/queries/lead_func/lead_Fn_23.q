SELECT LEAD(col4 ) OVER ( PARTITION BY col2 ORDER BY col0 nulls LAST ) LEAD_col4 FROM "fewRowsAllData.parquet"