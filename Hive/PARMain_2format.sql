add jar file:///home/afancy/smas-benchmark-1.0-SNAPSHOT.jar;
add jar file:///home/afancy/commons-math-2.2.jar;
SET hive.mapred.supports.subdirectories=true;
SET mapred.input.dir.recursive=true;
SET mapreduce.input.fileinputformat.input.dir.recursive=true;

--SET mapreduce.cluster.mapmemory.mb = -1;
--SET mapreduce.cluster.reducememory.mb = -1;
--SET mapreduce.map.memory.mb = -1;
--SET mapreduce.reduce.memory.mb = -1;
--SET mapreduce.cluster.mapmemory.mb = -1;
--SET mapreduce.jobtracker.maxreducememory.mb = -1;


--CREATE TEMPORARY FUNCTION par AS 'ca.uwaterloo.iss4e.hive.pointperrow.UDAFPAR';
CREATE TEMPORARY FUNCTION par as 'ca.uwaterloo.iss4e.hive.meterperrow.UDFPAR';

DROP TABLE IF EXISTS smas_par_results;
CREATE TABLE smas_par_results(meterid INT, result ARRAY<ARRAY<double>>);
--INSERT OVERWRITE TABLE smas_par_results SELECT meterid, par(reading) FROM tbl_firstformat GROUP BY meterid;
INSERT OVERWRITE TABLE smas_par_results SELECT meterid, par(reading) FROM tbl_secondformat;
