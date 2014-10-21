add jar file:///home/afancy/smas-benchmark-1.0-SNAPSHOT.jar;
add jar file:///home/afancy/commons-math-2.2.jar;
SET hive.mapred.supports.subdirectories=true;
SET mapred.input.dir.recursive=true;
SET mapreduce.input.fileinputformat.input.dir.recursive=true;

--CREATE TEMPORARY FUNCTION hist AS 'ca.uwaterloo.iss4e.hive.pointperrow.UDAFHistogram';
CREATE TEMPORARY FUNCTION hist as 'ca.uwaterloo.iss4e.hive.meterperrow.UDFHistogram';

DROP TABLE IF EXISTS smas_histogram_results;
CREATE TABLE smas_histogram_results(meterid INT, result ARRAY<INT>);
--INSERT OVERWRITE TABLE smas_histogram_results SELECT meterid, hist(reading) FROM tbl_firstformat GROUP BY meterid;
INSERT OVERWRITE TABLE smas_histogram_results SELECT meterid, hist(reading) FROM tbl_secondformat;

--create table smas_histogram_results_builtin as select meterid, histogram_numeric(reading, 10) as hist from smas_power_hourlyreading group by meterid;
