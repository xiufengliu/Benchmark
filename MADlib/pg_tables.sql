CREATE TABLE smas_power_hourlyreading
(
  meterid integer,
  readtime timestamp without time zone,
  reading numeric(10,2),
  temperature numeric(10,2)
);
-- Index: idx_smas_power_hourlyreading_meterid

-- DROP INDEX idx_smas_power_hourlyreading_meterid;

CREATE INDEX idx_smas_power_hourlyreading_meterid
  ON smas_power_hourlyreading
  USING btree
  (meterid);
ALTER TABLE smas_power_hourlyreading CLUSTER ON idx_smas_power_hourlyreading_meterid;

-- Index: idx_smas_power_hourlyreading_meterid_readtime

-- DROP INDEX idx_smas_power_hourlyreading_meterid_readtime;

CREATE INDEX idx_smas_power_hourlyreading_meterid_readtime
  ON smas_power_hourlyreading
  USING btree
  (meterid, readtime);

-- Index: idx_smas_power_hourlyreading_readtime

-- DROP INDEX idx_smas_power_hourlyreading_readtime;

CREATE INDEX idx_smas_power_hourlyreading_readtime
  ON smas_power_hourlyreading
  USING btree
  (readtime);

CREATE TABLE smas_meterids(meterid INTEGER);
