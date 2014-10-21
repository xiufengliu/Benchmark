CREATE OR REPLACE FUNCTION run_benchmark_similarity(seriesnum integer, windowsize integer)
  RETURNS void AS
$BODY$
DECLARE
  mids integer[];
  X float8[];
  Y float8[];
  minLength INTEGER;
  dotXY float8:=0.0;
  norm  float8:=0.0;
  normX  float8:=0.0;
  normY  float8:=0.0;
  starttime timestamp;
  endtime timestamp;
  -------------
  st timestamp;
  ed timestamp;
  
BEGIN

TRUNCATE smas_similarity_result;
starttime := '2011-01-01 00:00:00'::timestamp;
endtime := '2011-01-01 23:59:59'::timestamp + windowsize * interval '1 day';

st:=clock_timestamp();
	CREATE TEMPORARY Table temp_similarity (meterid1 integer, meterid2 integer, similarity float8) ON COMMIT DROP;
	
        SELECT array_agg(meterid) INTO mids FROM (SELECT meterid from smas_meterids limit seriesnum) A;
	
	FOR i IN 1..coalesce(array_length(mids, 1)-1, 0) LOOP
		SELECT array_agg(reading) INTO X FROM smas_power_hourlyreading WHERE meterid=mids[i] AND readtime BETWEEN starttime AND endtime;
		FOR j IN i+1..coalesce(array_length(mids, 1), 0) LOOP
			SELECT array_agg(reading) INTO Y FROM smas_power_hourlyreading WHERE meterid=mids[j] AND readtime BETWEEN starttime AND endtime;
			FOR n IN 1..coalesce(least(array_length(X, 1), array_length(Y, 1)), 0) LOOP
				dotXY := dotXY + X[n]*Y[n];
				normX := normX + X[n]*X[n];
				normY := normY + Y[n]*Y[n];
			END LOOP;
			norm := sqrt(normX) * sqrt(normY);
			IF norm>0 THEN
				INSERT INTO temp_similarity VALUES (mids[i], mids[j], dotXY/norm);
			END IF;
		END LOOP;	
	END LOOP;

	INSERT INTO smas_similarity_result 
	SELECT * FROM temp_similarity ORDER BY 3 desc LIMIT 10 ;
	
ed:=clock_timestamp();
	RAISE NOTICE 'Total time=%', ed-st;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION run_benchmark_similarity(integer, integer)
  OWNER TO afancy;
