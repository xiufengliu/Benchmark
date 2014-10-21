add jar file:///home/afancy/smas-benchmark-1.0-SNAPSHOT.jar;
add jar file:///home/afancy/commons-math-2.2.jar;

CREATE TEMPORARY FUNCTION cosine as 'ca.uwaterloo.iss4e.hive.meterperrow.UDFCosine';
CREATE TEMPORARY FUNCTION colasc as 'ca.uwaterloo.iss4e.hive.meterperrow.UDFColAsc';


DROP TABLE smas_similarity_results;

CREATE TABLE smas_similarity_results(meterid array<int>, similarity double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;; 

insert overwrite table smas_similarity_results select distinct colasc(a.meterid, b.meterid) as meterids, cosine(a.reading, b.reading) as similarity from smas_reading_arr a, smas_reading_arr b WHERE a.meterid<>b.meterid order by similarity desc limit 10;
