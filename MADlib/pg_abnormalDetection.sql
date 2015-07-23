CREATE TABLE tstable
(
  meterid integer,
  readtime timestamp without time zone,
  reading numeric(10,2),
  temperature numeric(10,2)
);

CREATE INDEX idx_tstable_meterid
  ON tstable
  USING btree
  (meterid);
ALTER TABLE tstable CLUSTER ON idx_tstable_meterid;

CREATE INDEX idx_tstable_meterid_readtime
  ON tstable
  USING btree
  (meterid, readtime);


CREATE INDEX idx_tstable_readdate
  ON tstable
  USING btree
  ((readtime::date));


CREATE INDEX idx_tstable_readtime
  ON tstable
  USING btree
  (readtime);

CREATE TABLE neighbors
(
  meterid integer,
  neighbor integer
);


CREATE INDEX neighbors_meterid_idx
  ON neighbors
  USING btree
  (meterid);

CREATE TABLE parx_parameters
(
  meterid integer NOT NULL,
  season integer NOT NULL,
  coef double precision[],
  CONSTRAINT parx_parameters_pkey PRIMARY KEY (meterid, season)
);

CREATE TABLE normal_distribution
(
  meterid integer NOT NULL,
  mean double precision,
  stdev double precision,
  CONSTRAINT normal_distribution_pkey PRIMARY KEY (meterid)
);

CREATE OR REPLACE FUNCTION probability(x double precision, mean double precision, stdev double precision)
  RETURNS double precision AS
$BODY$
BEGIN
	BEGIN
		return power(exp(1.0::numeric), -1*(x-mean)^2/(2*stdev^2))/(stdev*sqrt(2*pi()));
	EXCEPTION 
	 WHEN others THEN
		return -1;
	END;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;


CREATE OR REPLACE FUNCTION training_ByPARX(startdate date, endend date)
  RETURNS void AS
$BODY$
DECLARE
  	st TIMESTAMP;
	ed TIMESTAMP;
	r meterids%rowtype;
BEGIN
	TRUNCATE parx_parameters;
	TRUNCATE normal_distribution;

	st:=clock_timestamp();
	INSERT INTO parx_parameters 
	SELECT meterid, season, (madlib.linregr(yt, array[1, x1,x2,x3,xt1,xt3,xt3])).coef FROM (
		SELECT  meterid,
			extract(hour from readtime) as season,
			reading as yt,
			LEAD(reading, 1) OVER (PARTITION BY meterid,extract(hour from readtime) order by readtime desc) as x1,
			LEAD(reading, 2) OVER (PARTITION BY meterid,extract(hour from readtime) order by readtime desc) as x2,
			LEAD(reading, 3) OVER (PARTITION BY meterid,extract(hour from readtime) order by readtime desc) as x3, 
			CASE WHEN temperature>20 THEN temperature-20 ELSE 0 END AS xt1, 
			CASE WHEN temperature<16 THEN 16-temperature ELSE 0 END AS xt2,
			CASE WHEN temperature<5 THEN 5-temperature ELSE 0 END AS xt3
		FROM tstable
		WHERE readtime::date BETWEEN $1 AND $2
	) A WHERE yt IS NOT NULL AND x1 IS NOT NULL AND x2 IS NOT NULL
	GROUP BY 1, 2;

	INSERT INTO normal_distribution
	SELECT 
		meterid,
		avg(D.dailynorm) As mean,
		stddev(D. dailynorm) AS stddev
	FROM(	
		SELECT
			C.meterid,
			C.date,
			sqrt(sum(norm)) AS dailynorm
		FROM(
			SELECT  A.meterid,
				readtime::date as date,
				(reading -
				(B.coef[1] + 
				B.coef[2]*LEAD(reading, 1) OVER (PARTITION BY A.meterid,extract(hour from readtime) order by readtime desc) +
				B.coef[3]*LEAD(reading, 2) OVER (PARTITION BY A.meterid,extract(hour from readtime) order by readtime desc) +
				B.coef[4]*LEAD(reading, 3) OVER (PARTITION BY A.meterid,extract(hour from readtime) order by readtime desc) +
				B.coef[5]*CASE WHEN temperature>20 THEN temperature-20 ELSE 0 END +
				B.coef[6]*CASE WHEN temperature<16 THEN 16-temperature ELSE 0 END +
				B.coef[7]*CASE WHEN temperature<5 THEN 5-temperature ELSE 0 END))^2 AS norm
			FROM  tstable A, parx_parameters B
			WHERE A.meterid=B.meterid AND
			    extract(hour from readtime)=B.season AND 
			    readtime::date  BETWEEN  $1 AND $2
		) C 
		WHERE C.norm IS NOT NULL
		GROUP BY 1,2
	) D GROUP BY 1;
	ed:=clock_timestamp();
	
	RAISE NOTICE 'training_ByPARX time=%',ed-st;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
  
CREATE OR REPLACE FUNCTION detect_byPARX(today date, threshold double precision)
  RETURNS void AS
$BODY$
DECLARE
	st TIMESTAMP;
	ed TIMESTAMP;
	probability float8;
	r meterids%rowtype;
BEGIN
	--TRUNCATE TABLE result;
	DELETE FROM result WHERE readdate=$1;
	
	DELETE FROM tstable WHERE readtime::date < $1-3*interval'1day';
	
	st:=clock_timestamp();
	INSERT INTO result
	SELECT 	C.meterid,
		$1 AS readdate,
		probability(C.l2dist, D.mean, D.stdev)
	FROM
	(
		SELECT  A.meterid, 
			sqrt(sum((yt-(coef[1]+A.x1*coef[2]+A.x2*coef[3]+A.x3*coef[4]+xt1*coef[5]+xt2*coef[6]+xt3*coef[7]))^2)) AS l2dist 
		FROM (
			SELECT
				meterid,
				extract(hour from readtime) AS season,
				reading as yt,
				LEAD(reading, 1) OVER (PARTITION BY meterid,extract(hour from readtime) order by readtime desc) as x1,
				LEAD(reading, 2) OVER (PARTITION BY meterid,extract(hour from readtime) order by readtime desc) as x2,
				LEAD(reading, 3) OVER (PARTITION BY meterid,extract(hour from readtime) order by readtime desc) as x3, 
				CASE WHEN temperature>20 THEN temperature-20 ELSE 0 END AS xt1, 
				CASE WHEN temperature<16 THEN 16-temperature ELSE 0 END AS xt2,
				CASE WHEN temperature<5 THEN 5-temperature ELSE 0 END AS xt3
				FROM  tstable
			WHERE readtime::date BETWEEN $1-3*interval'1day' AND $1
		     ) A, parx_parameters B
		WHERE A.meterid=B.meterid AND A.season=B.season AND (A.yt IS NOT NULL AND A.x1 IS NOT NULL AND A.x2 IS NOT NULL AND A.x3 IS NOT NULL) 
		GROUP BY 1
	) C, normal_distribution D
	WHERE C.meterid=D.meterid AND D.stdev>0;

	DELETE FROM result WHERE readdate=$1 AND probability>$2;
	ed:=clock_timestamp();
	RAISE NOTICE 'detect_byPARX time=%',ed-st;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
  
-------------------------------------

CREATE OR REPLACE FUNCTION training_byNeighborAvg(startdate date, enddate date)
  RETURNS void AS
$BODY$
DECLARE
	st TIMESTAMP;
	ed TIMESTAMP;
BEGIN
	TRUNCATE normal_distribution;

	st:=clock_timestamp();
	INSERT INTO normal_distribution 
	SELECT 	meterid, 
		avg(l2dist), 
		stddev(l2dist)
	FROM (
		SELECT  C.meterid,
			C.readdate, 
			sqrt(sum((C.myReading-D.avgNeighborReading)^2)) AS l2dist
		FROM (
			SELECT  A.meterid,
				B.readtime::date AS readdate,
				extract(hour from B.readtime) AS season, 
				B.reading AS myReading 
			FROM  neighbors A, tstable B
			WHERE A.meterid=B.meterid AND  B.readtime::date between $1 and $2
		) C,
		(
			SELECT  A.meterid,
				B.readtime::date AS readdate,
				extract(hour from B.readtime) AS season, 
				avg(B.reading)  AS avgNeighborReading
			FROM neighbors A, tstable B
			WHERE A.neighbor=B.meterid AND A.meterid<>B.meterid AND B.readtime::date between $1 and $2
			GROUP BY 1,2,3 ORDER BY 1,2,3
		) D
		WHERE C.meterid=D.meterid AND C.readdate=D.readdate AND C.season=D.season
		GROUP BY 1,2 ORDER BY 1,2
	) E GROUP BY 1;
	ed:=clock_timestamp();
	RAISE NOTICE 'training time=%',ed-st;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;



CREATE OR REPLACE FUNCTION detect_byNeighborAvg(today date, threshold double precision)
  RETURNS void AS
$BODY$
DECLARE
	st TIMESTAMP;
	ed TIMESTAMP;
BEGIN

	DELETE FROM result WHERE readdate=$1;
	
	st:=clock_timestamp();
	INSERT INTO result
	SELECT 	
		E.meterid,
		$1 AS readdate,
		probability(E.l2dist, D.mean, d.stdev)
	FROM (
		SELECT 
			C.meterid,
			sqrt(sum((C.reading-D.avgNeighborReading)^2)) AS l2dist
		FROM
		 tstable C,
		(
			SELECT 
				A.meterid,
				extract(hour from B.readtime) AS season, 
				avg(B.reading)  AS avgNeighborReading
			FROM neighbors A, tstable B
			WHERE A.neighbor=B.meterid AND A.meterid<>B.meterid AND B.readtime::date=$1 
			GROUP BY 1, 2 ORDER BY 1,2
		) D WHERE C.meterid=D.meterid AND extract(hour from C.readtime)=D.season AND C.readtime::date=$1
		GROUP BY 1
	) E, normal_distribution D
	WHERE E.meterid=D.meterid AND D.stdev>0;
	
	DELETE FROM result WHERE readdate=$1 AND probability>$2;

	ed:=clock_timestamp();
	RAISE NOTICE 'detect time=%',ed-st;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

