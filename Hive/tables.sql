drop table smas_power_hourlyreading;

create table tbl_firstformat(meterid int, readtime string, reading double, temperature double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

create table smas_power_hourlyreading (meterid int, readtime string, reading double, temperature double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

create table smas_power_hourlyreading_100meterperfile (meterid int, readtime string, reading double, temperature double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

create table smas_power_hourlyreading (meterid int, reading string, temperature string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;


create table smas_power_hourlyreading (meterid int, reading array<double>, temperature array<double>) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

CREATE TABLE smas_power_hourlyreading1(meterid int, readtime string, reading double, temperature double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.contrib.fileformat.UnsplitableTextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat';


create table smas_power_hourlyreading_parquet (meterid int, readtime string, reading double, temperature double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS PARQUET;

CREATE TABLE parquet_test ( id int, str string, mp MAP<STRING,STRING>, lst ARRAY<STRING>, strct STRUCT<A:STRING,B:STRING>) PARTITIONED BY (part string)STORED AS PARQUET;

create table smas_reading_arr(meterid INT, reading array<DOUBLE>);
create table smas_threel_results(meterid int, result array<array<double>>);
create table smas_histogram_results(meterid int, result array<int>);
create table smas_par_results(meterid int, result array<array<double>>);
create table smas_similarity_results(meterid array<int>, similarity double);

--https://github.com/cartershanklin/hive-testbench/blob/master/settings/init.sql
set mapred.max.split.size=51200000000; 
set mapreduce.input.fileinputformat.split.minsizee=2400000000;              
set mapreduce.input.fileinputformat.split.minsize.per.rack=2400000000;      
set mapreduce.input.fileinputformat.split.minsize.per.node=2400000000; 

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
