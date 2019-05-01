-- Create a column for postgis geometry
SELECT AddGeometryColumn('location', 'geom', 4326, 'POINT', 2);
ALTER TABLE location ADD COLUMN geog geometry(Geometry,4326);

-- Index
CREATE INDEX idx_geom_location ON location USING gist(geom);
CREATE INDEX idx_geog_location ON location USING gist(geog);

-- Trigger
CREATE OR REPLACE FUNCTION function_update()
  RETURNS trigger AS
$BODY$
BEGIN
    new.geom := ST_SetSRID(ST_MakePoint(new.longitude, new.latitude), 4326);
    RETURN new;
END;
$BODY$
LANGUAGE plpgsql;

DROP TRIGGER pointinsert ON location;

CREATE TRIGGER pointinsert
  BEFORE INSERT
  ON location
  FOR EACH ROW
  EXECUTE PROCEDURE function_update();

-- Trigger
CREATE OR REPLACE FUNCTION function_update_geog()
  RETURNS trigger AS
$BODY$
BEGIN
    new.geog := ST_SetSRID(ST_MakePoint(new.longitude, new.latitude),4326)::geography;
    RETURN new;
END;
$BODY$
LANGUAGE plpgsql;

DROP TRIGGER pointinsertgeog on location;

CREATE TRIGGER pointinsertgeog
  BEFORE INSERT
  ON location
  FOR EACH ROW
  EXECUTE PROCEDURE function_update_geog();

-- FUNCTION THAT CONVERTS LONG/LAT TO GEOM_POINT
CREATE OR REPLACE FUNCTION text_to_geom_point(d1 double precision, d2 double precision) RETURNS geometry(Geometry,4326) AS $$
        BEGIN
                RETURN ST_SetSRID(ST_Point(d1, d2),4326)::geography;
        END;
$$ LANGUAGE plpgsql;

-- --TESTING
--SELECT ST_Distance(gg1, gg2) As spheroid_dist, ST_Distance(gg1, gg2, false) As sphere_dist
--FROM (SELECT
--	ST_GeographyFromText('SRID=4326;POINT(-3.60013560280968 40.4806216155513)') As gg1,
--	ST_GeographyFromText('SRID=4326;POINT(-3.6 40)') As gg2
--	) As foo  ;
--
--select  id, altitude, latitude, longitude, timestamp, user_id from location
--WHERE ST_Distance(
--ST_SetSRID(ST_Point(longitude, latitude),4326)::geography,
--ST_GeographyFromText('SRID=4326;POINT(-3.6 40)'), false) <=53000;
--
--select  id, altitude, latitude, longitude, timestamp, user_id from location
--WHERE ST_Distance(
--ST_SetSRID(ST_Point(longitude, latitude),4326)::geography,
--text_to_geom_point(-3.6, 40), false) <=53000;


--select * from location
--WHERE ST_Distance(geog, ST_GeographyFromText('SRID=4326;POINT(-3.6 40)'), false) <=53000;

-- UPDATE tables with correct timestamp and save raw_timestamp
set datestyle=ISO, YMD;
ALTER TABLE location ALTER COLUMN raw_timestamp TYPE text;
UPDATE SET raw_timestamp = SELECT EXTRACT(EPOCH FROM timestamp) from location;

select count(*) from location when raw_timestamp IS NULL;
SELECT raw_timestamp, to_char(raw_timestamp, 'YYYY-MM-DD HH24:MI:SS') from location LIMIT 1;
ALTER TABLE location ALTER COLUMN timestamp TYPE timestamptz;
select (raw_timestamp/1000)::timestamp from location limit 1;
UPDATE SET timestamp = to_timestamp(raw_timestamp::bigint / 1000.0) from location LIMIT 1;

select * from location WHERE raw_timestamp LIKE '%.%';
update location SET timestamp = to_timestamp(raw_timestamp::bigint / 1000.0) WHERE raw_timestamp NOT LIKE '%.%';

