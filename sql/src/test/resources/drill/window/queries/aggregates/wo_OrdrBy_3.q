SELECT MIN(col_int) OVER (PARTITION BY col_vchar_52) min_int, col_vchar_52, col_int FROM "smlTbl.parquet" WHERE col_vchar_52 = "DXXXXXXXXXXXXXXXXXXXXXXXXXEXXXXXXXXXXXXXXXXXXXXXXXXF"