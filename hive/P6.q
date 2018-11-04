--Output the playerID’s of the top 3 players inclusive 2005 through 2009 with max:
--(number of hits/number of at bats – (number of errors/ number of games)
DROP TABLE IF EXISTS fielding;
CREATE EXTERNAL TABLE IF NOT EXISTS fielding(
    id STRING, yr INT, team STRING,
    lgID STRING, pos STRING, g FLOAT,
    gs INT, innouts INT, po INT, a INT,
    errors FLOAT, dp INT, pb INT, wp INT,
    sb INT, cs INT, zr INT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/maria_dev/hive/fielding'
tblproperties("skip.header.line.count"="1");

DROP TABLE IF EXISTS batting;
CREATE EXTERNAL TABLE IF NOT EXISTS batting(
    id STRING, yr INT, team STRING, league STRING, games INT, 
    ab INT, runs INT, hits INT, doubles INT, triples INT, homeruns INT, 
    rbi INT, sb INT, cs INT, walks INT, strikeouts INT, ibb INT, 
    hbp INT, sh INT, sf INT, gidp INT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION '/user/maria_dev/hive/batting'
tblproperties("skip.header.line.count"="1");

DROP TABLE IF EXISTS field_filt;
CREATE TABLE field_filt(id STRING, errors FLOAT, games FLOAT, ER FLOAT);
INSERT INTO TABLE field_filt
    SELECT f.id as id, sum(f.errors) as errors, sum(f.g) as games, sum(f.errors)/sum(f.g) as ER
    FROM fielding f
    WHERE f.yr>=2005 and f.yr<=2009
    GROUP BY f.id
    HAVING games>=20;

DROP TABLE IF EXISTS bat_filt;
CREATE TABLE bat_filt(id STRING, t_hits FLOAT, bats FLOAT, HR FLOAT);
INSERT INTO TABLE bat_filt
    SELECT b.id as id, sum(b.hits) as t_hits, sum(b.ab) as bats,  sum(b.hits)/sum(b.ab) as HR
    FROM batting b
    WHERE b.yr>=2005 and b.yr<=2009
    GROUP BY b.id
    HAVING bats>=40;

DROP TABLE IF EXISTS aggr;
CREATE TABLE aggr(id STRING, HR FLOAT, ER FLOAT, RATE FLOAT);
INSERT INTO TABLE aggr
    SELECT b.id, b.HR as HR, f.ER as ER, b.HR-f.ER as RATE
    FROM bat_filt b, field_filt f
    WHERE b.id=f.id
    ORDER BY RATE DESC;

SELECT id
FROM aggr
LIMIT 3;