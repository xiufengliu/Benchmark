add jar file:///home/afancy/smas-benchmark-1.0-SNAPSHOT.jar;
add jar file:///home/afancy/commons-math-2.2.jar;

CREATE TEMPORARY FUNCTION collect_array as 'ca.uwaterloo.iss4e.hive.pointperrow.UDAFCollectArray';

CREATE TABLE IF NOT EXISTS tbl_firstformat(meterid int, readtime string, reading double, temperature double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;


DROP TABLE tbl_secondformat;

--Prepare data for spark, meter per line
CREATE TABLE IF NOT EXISTS tbl_secondformat(meterid int, reading string, temperature string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
INSERT OVERWRITE TABLE tbl_secondformat SELECT meterid, collect_array(reading) AS reading, collect_array(temperature) AS temperature FROM tbl_firstformat  GROUP BY meterid;
