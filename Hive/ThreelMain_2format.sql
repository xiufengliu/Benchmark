add jar file:///home/afancy/smas-benchmark-1.0-SNAPSHOT.jar;
add jar file:///home/afancy/commons-math-2.2.jar;
SET hive.mapred.supports.subdirectories=true;
SET mapred.input.dir.recursive=true;
SET mapreduce.input.fileinputformat.input.dir.recursive=true;

--SET hive.exec.compress.output=true;
--SET mapred.max.split.size=256000000;
--SET mapred.output.compression.type=BLOCK;
--SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
--SET hive.exec.dynamic.partition.mode=nonstrict;
--SET hive.exec.dynamic.partition=true;

--SET mapred.job.reuse.jvm.num.tasks=100;
--SET fs.hdfs.impl.disable.cache=true;
--SET fs.file.impl.disable.cache=true; 

--set mapred.max.split.size=51200000000; 
--set mapreduce.input.fileinputformat.split.minsizee=2400000000;              
--set mapreduce.input.fileinputformat.split.minsize.per.rack=2400000000;      
--set mapreduce.input.fileinputformat.split.minsize.per.node=2400000000; 

--CREATE TEMPORARY FUNCTION threel as 'ca.uwaterloo.iss4e.hive.pointperrow.UDAFThreel';
CREATE TEMPORARY FUNCTION threel as 'ca.uwaterloo.iss4e.hive.meterperrow.UDFThreel';
--CREATE TEMPORARY FUNCTION threel as 'ca.uwaterloo.iss4e.hive.meterperfile.UDTFThreel';

DROP TABLE IF EXISTS smas_threel_results;
CREATE TABLE smas_threel_results(meterid INT, RESULT ARRAY<ARRAY<double>>);

--INSERT OVERWRITE TABLE smas_threel_results SELECT meterid, threel(reading, temperature)  FROM tbl_firstformat GROUP BY meterid;
INSERT OVERWRITE TABLE smas_threel_results SELECT meterid, threel(reading, temperature)  FROM tbl_secondformat;


