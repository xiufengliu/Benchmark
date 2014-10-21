DROP TYPE smas_points_type  CASCADE;
CREATE TYPE smas_points_type AS (
    pointA            float8[],
    pointB            float8[],
    pointC            float8[],
    pointD           float8[]
);

-- Function: smas_power_quantiled_threel(integer, double precision[], double precision[])

-- DROP FUNCTION smas_power_quantiled_threel(integer, double precision[], double precision[]);

CREATE OR REPLACE FUNCTION smas_power_quantiled_threel(size integer, temarr double precision[], qua double precision[])
  RETURNS smas_points_type AS
$BODY$
	DECLARE
	  rq1 float8;
	  rq2 float8;
	  rq3 float8;
	  total_or2 float8:= 'Infinity'::float8;
	  x1 float8;
	  x2 float8;ox2 float8;
	  x3 float8;ox3 float8;
	  xn float8;
	  y1 float8;
	  y2 float8;
	  y3 float8;
	  yn float8;
	  coef1 float8[];coeftmp1 float8[];
	  coef2 float8[];coeftmp2 float8[];
	  coef3 float8[];coeftmp3 float8[];
	  m2 float8;
	  m3 float8;
	  h2 float8;
	  h3 float8;
	  p2s float8[][];
	  p3s float8[][];
	  pos int4;

	  o_p1 float8[];
	  o_p2 float8[];
	  o_p3 float8[];
	  o_p4 float8[];

	  err1 float8:=0;
	  err2 float8:=0;
	  err3 float8:=0;
	  result smas_points_type;
BEGIN
	FOR s1 IN 10..20 LOOP
		SELECT (lr).coef, (lr).r2 INTO coeftmp1, rq1 FROM (
			SELECT (madlib.linregr(A.q, ARRAY[1.0, t])) AS lr  from (select unnest(temarr) as t, unnest(qua) as q) AS A WHERE A.t<=s1
		) AS lrq;

		FOR s2 IN (s1+5)..(s1+10) LOOP
			SELECT (lr).coef, (lr).r2 INTO coeftmp2, rq2 FROM (
			  SELECT (madlib.linregr(A.q, ARRAY[1.0, t])) AS lr  FROM (SELECT unnest(temarr) AS t, unnest(qua) AS q) AS A WHERE A.t BETWEEN s1 AND s2
			) AS lrq;
			SELECT (lr).coef, (lr).r2 INTO coeftmp3, rq3 FROM (
			  SELECT (madlib.linregr(A.q, ARRAY[1.0, t])) AS lr  FROM (SELECT unnest(temarr) AS t, unnest(qua) AS q) AS A WHERE A.t>=s2
			) AS lrq;
		   IF (rq1+rq2+rq3<total_or2) THEN
                ox2 := s1;
                ox3 := s2;
                coef1 := coeftmp1;
                coef2 := coeftmp2;
                coef3 := coeftmp3;
                total_or2 := rq1+rq2+rq3;
		   END IF;
		END LOOP;
	END LOOP;

    --RAISE NOTICE 'ox2, ox3=%, %', ox2, ox3;
    --RAISE NOTICE 'coef1, coef2, coef3=%, %, %', coef1, coef2, coef3;

	m2 :=((coef1[1]+coef1[2]*ox2)+(coef2[1]+coef2[2]*ox2))/2.0;
	h2 :=((coef2[1]+coef2[2]*ox2)-(coef1[1]+coef1[2]*ox2))/2.0;
    --RAISE NOTICE 'm2, h2=%, %', m2,h2;
	m3 :=((coef2[1]+coef2[2]*ox3)+(coef3[1]+coef3[2]*ox3))/2.0;
	h3 :=((coef3[1]+coef3[2]*ox3)-(coef2[1]+coef2[2]*ox3))/2.0;
    --RAISE NOTICE 'm3, h3=%, %', m3,h3;
	p2s := ARRAY[[ox2-0.5, m2+h2]]||ARRAY[ox2,m2+h2]||ARRAY[ox2+0.5, m2+h2]
		  ||ARRAY[ox2-0.5, m2]||ARRAY[ox2,m2]||ARRAY[ox2+0.5,m2]
		  ||ARRAY[ox2-0.5,m2-h2]||ARRAY[ox2,m2-h2]||ARRAY[ox2+0.5,m2-h2];

	p3s := ARRAY[[ox3-0.5, m3+h3]]||ARRAY[ox3,m3+h3]||ARRAY[ox3+0.5, m3+h3]
		  ||ARRAY[ox3-0.5, m3]||ARRAY[ox3,m3]||ARRAY[ox3+0.5,m3]
		  ||ARRAY[ox3-0.5,m3-h3]||ARRAY[ox3,m3-h3]||ARRAY[ox3+0.5,m3-h3];

    --RAISE NOTICE 'p2s=%', p2s;
    --RAISE NOTICE 'p3s=%', p3s;

	total_or2 :=  'Infinity'::float8;
	FOR i in 1..9 LOOP
	   x1 := temArr[1];
	   y1 := coef1[1]+coef1[2]*x1;
	   x2 := unnest(p2s[i:i][1:1]);
	   y2 := unnest(p2s[i:i][2:2]);
	   -- y=y1+(y2-y1)/(x2-x1)*(x-x1) --line 1
	   err1 := 0;
	   FOR idx IN 1..size LOOP
            IF temArr[idx]<=x2 THEN
               err1 := err1 + power(qua[idx]-(y1+(y2-y1)/(x2-x1)*(temArr[idx]-x1)),2);
            ELSE
               pos:=idx;
               exit;
            END IF;
	   END LOOP;

	   FOR j in 1..9 LOOP
		x3 := unnest(p3s[j:j][1:1]);
		y3 := unnest(p3s[j:j][2:2]);
		xn := temArr[size];
		yn := coef3[1]+coef3[2]*xn;
		-- y=y2+(y3-y2)/(x3-x2)*(x-x2) --line 2
		-- y=y3+(yn-y3)/(xn-x3)*(x-x3) --line 3

        err2 := 0;
        err3 := 0;
		FOR idx IN pos..size LOOP
			if temArr[idx]<=x3 then
			   err2 := err2 + power(qua[idx]-(y2+(y3-y2)/(x3-x2)*(temArr[idx]-x2)),2);
			else
			   err3 := err3 + power(qua[idx]-(y3+(yn-y3)/(xn-x3)*(temArr[idx]-x3)),2);
			end if;
		END LOOP;
	 --RAISE notice 'err1+err2+err3=%, %', err1+err2+err3,total_or2;
		IF (err1+err2+err3<total_or2) THEN
		   total_or2 := err1+err2+err3;
		   o_p1 := ARRAY[x1, y1];
		   o_p2 := ARRAY[x2, y2];
		   o_p3 := ARRAY[x3, y3];
		   o_p4 := ARRAY[xn, yn];
		END IF;
	   END LOOP;
	END LOOP;

	SELECT o_p1, o_p2, o_p3, o_p4 INTO result;
	RETURN result;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;


-- Function: smas_power_threel(integer)



-- Function: smas_power_threel(integer, timestamp without time zone, timestamp without time zone)

-- DROP FUNCTION smas_power_threel(integer, timestamp without time zone, timestamp without time zone);

CREATE OR REPLACE FUNCTION smas_power_threel(mid integer, starttime timestamp without time zone, endtime timestamp without time zone)
  RETURNS smas_points_type AS
$BODY$
DECLARE
  temArr float8[];
  q50Arr float8[];
  size INTEGER;
BEGIN
	SELECT  array_agg(A.tem),
	        array_agg(A.readarr[floor(A.cnt*0.5)])
	        INTO temArr, q50Arr
	 FROM (
		 SELECT
			round(temperature) AS tem,
			count(*) AS cnt,
			array_agg(reading ORDER BY reading) AS readarr
		 FROM smas_power_hourlyreading  
		 WHERE meterid=$1 AND readtime BETWEEN $2 AND $3
		 GROUP BY tem HAVING count(*)>20 ORDER BY tem
	) AS A;

    size := array_length(temArr,1);
    IF size>2 THEN
      RETURN smas_power_quantiled_threel(size, temArr, q50Arr);
    ELSE
     RETURN NULL;
    END IF;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;



-- Function: run_benchmark_threel(integer, integer)

-- DROP FUNCTION run_benchmark_threel(integer, integer);

CREATE OR REPLACE FUNCTION run_benchmark_threel(seriesnum integer, starttime timestamp, windowsize integer)
  RETURNS void AS
$BODY$
DECLARE
  	st TIMESTAMP;
	ed TIMESTAMP;
	r smas_meterids%rowtype;
	endtime TIMESTAMP;
BEGIN

TRUNCATE TABLE smas_threel_result;
endtime := starttime + windowsize * interval '1 day';

st:=clock_timestamp();
	FOR r IN SELECT meterid FROM smas_meterids limit seriesnum
	LOOP
		INSERT INTO smas_threel_result SELECT r.meterid, smas_power_threel(r.meterid, starttime, endtime);
	END LOOP;
ed:=clock_timestamp();
RAISE NOTICE 'time=%', ed-st;
RETURN;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;


