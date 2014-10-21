-- Function: run_benchmark_par(integer, integer)

-- DROP FUNCTION run_benchmark_par(integer, integer);

CREATE OR REPLACE FUNCTION run_benchmark_par(seriesnum integer, starttime timestamp, windowsize integer)
  RETURNS void AS
$BODY$
DECLARE
  	st TIMESTAMP;
	ed TIMESTAMP;
	r smas_meterids%rowtype;
	
	endtime TIMESTAMP;
BEGIN

TRUNCATE TABLE smas_par_result;

endtime := starttime + windowsize * interval '1 day';

st:=clock_timestamp();
	FOR r IN SELECT meterid FROM smas_meterids limit seriesnum
	LOOP
		INSERT INTO smas_par_result 
		SELECT r.meterid, season, (madlib.linregr(yt, array[x1,x2,x3])).coef FROM (
		SELECT 	  extract(hour from readtime) as season,
			  reading as yt,
			  LEAD(reading, 1) OVER (PARTITION BY meterid order by readtime desc) as x1,
			  LEAD(reading, 2) OVER (PARTITION BY meterid order by readtime desc) as x2,
			  LEAD(reading, 3) OVER (PARTITION BY meterid order by readtime desc) as x3 
		FROM  smas_power_hourlyreading 
		WHERE smas_power_hourlyreading.meterid=r.meterid AND readtime BETWEEN starttime AND endtime
		) A WHERE yt IS NOT NULL AND x1 IS NOT NULL AND x2 IS NOT NULL
		GROUP BY 1, 2;
	END LOOP;
ed:=clock_timestamp();
RAISE NOTICE 'time=%',ed-st;

END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
