add jar file:///home/afancy/smas-benchmark-1.0-SNAPSHOT.jar;
add jar file:///home/afancy/commons-math-2.2.jar;
SET hive.mapred.supports.subdirectories=true;
SET mapred.input.dir.recursive=true;
SET mapreduce.input.fileinputformat.input.dir.recursive=true;


CREATE TEMPORARY FUNCTION hist as 'ca.uwaterloo.iss4e.hive.meterperfile.UDTFHistogram';

DROP TABLE IF EXISTS smas_histogram_results;
CREATE TABLE smas_histogram_results(result string);

INSERT OVERWRITE TABLE smas_histogram_results SELECT hist(meterid, reading) FROM tbl_firstformat;
