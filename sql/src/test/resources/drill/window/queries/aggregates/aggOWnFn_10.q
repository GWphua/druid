SELECT MAX(firstVal_c1) FROM (SELECT FIRST_VALUE(c1) OVER(PARTITION BY c2 ORDER BY c1) firstVal_c1 , c2 FROM "tblWnulls.parquet") sub_query