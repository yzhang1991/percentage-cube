CREATE LIBRARY SumWithNull AS '/home/dbadmin/percentage-cube/SumWithNull.so';
CREATE AGGREGATE FUNCTION sumnull AS LANGUAGE 'C++' NAME 'SumWithNullFactory' LIBRARY SumWithNull;
