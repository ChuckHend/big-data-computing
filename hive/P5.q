--Output the playerID of the player who had the most errors in all seasons combined.
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

DROP TABLE IF EXISTS players;
CREATE TABLE IF NOT EXISTS players(id STRING, tot_errors FLOAT);
INSERT INTO TABLE players
    SELECT id, sum(errors) as tot_errors
    FROM fielding
    GROUP BY id
    ORDER BY tot_errors DESC;

SELECT id
FROM (
    SELECT id, RANK() OVER (ORDER BY tot_errors DESC) as rk
    FROM players) tab
WHERE tab.rk=1;
