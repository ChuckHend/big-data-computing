--Output the team that had the most errors in 2001.
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

DROP TABLE IF EXISTS errors;
CREATE TABLE IF NOT EXISTS errors(team STRING, tot_errors INT);
INSERT INTO TABLE errors
    SELECT f.team, sum(f.errors) as tot_errors
    FROM fielding f
    WHERE f.yr=2001
    GROUP BY f.team
    ORDER BY tot_errors DESC;

SELECT team
FROM (
    SELECT team, RANK() OVER (ORDER BY tot_errors DESC) as rk
    FROM errors) tab
WHERE tab.rk=1;