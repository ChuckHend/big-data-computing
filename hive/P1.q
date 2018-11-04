--Output the birth city of the player who had the most at bats (AB) in his career.
DROP TABLE IF EXISTS batting;
CREATE EXTERNAL TABLE IF NOT EXISTS batting(
    id STRING, year INT, teamID STRING, lgID STRING, games INT, 
    ab INT, runs INT, hits INT, doubles INT, triples INT, homeruns INT, 
    rbi INT, sb INT, cs INT, walks INT, strikeouts INT, ibb INT, 
    hbp INT, sh INT, sf INT, gidp INT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/maria_dev/hive/batting'
tblproperties("skip.header.line.count"="1");

DROP TABLE IF EXISTS master;
CREATE EXTERNAL TABLE master(
    id STRING, byear INT, bmonth INT, bday INT, bcountry STRING, 
    bstate STRING, bcity STRING, dyear INT, dmonth INT, dday INT, 
    dcountry STRING, dstate STRING, dcity STRING, fname STRING, 
    lname STRING, name STRING, weight INT, height INT, bats STRING, 
    throws STRING, debut STRING, finalgame STRING, retro STRING, bbref STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION '/user/maria_dev/hive/master'
tblproperties("skip.header.line.count"="1");

DROP TABLE IF EXISTS best_batters;
CREATE TABLE IF NOT EXISTS best_batters(id STRING, bcity STRING, sum_at_bats INT);
INSERT INTO TABLE best_batters
    SELECT b.id as id, m.bcity as bcity, SUM(b.AB) as total_at_bats
    FROM batting b, master m
    WHERE b.id=m.id
    GROUP BY b.id, m.bcity
    ORDER BY total_at_bats DESC;

SELECT b.bcity
FROM best_batters b,
    (SELECT max(sum_at_bats) as max_bats
    FROM best_batters) max_bats
WHERE b.sum_at_bats=max_bats.max_bats;