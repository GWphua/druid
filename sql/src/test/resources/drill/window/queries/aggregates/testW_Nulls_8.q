SELECT c1, c2, w_COUNT FROM ( SELECT c1, c2, COUNT ( c1 ) OVER ( PARTITION BY c2 ORDER BY c1 ASC nulls first ) w_COUNT FROM "tblWnulls.parquet" ) sub_query WHERE w_COUNT IS null