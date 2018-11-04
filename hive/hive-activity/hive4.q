--Of the right-handed batters who were born in October and died in 2011, which one had the most hits in his career?
DROP TABLE IF EXISTS batting;
CREATE EXTERNAL TABLE IF NOT EXISTS batting(
    id STRING, year INT, team STRING, league STRING, games INT, 
    ab INT, runs INT, hits INT, doubles INT, triples INT, homeruns INT, 
    rbi INT, sb INT, cs INT, walks INT, strikeouts INT, ibb INT, 
    hbp INT, sh INT, sf INT, gidp INT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION '/user/maria_dev/hivetest/batting';

DROP TABLE IF EXISTS master;
CREATE EXTERNAL TABLE master(
    id STRING, byear INT, bmonth INT, bday INT, bcountry STRING, 
    bstate STRING, bcity STRING, dyear INT, dmonth INT, dday INT, 
    dcountry STRING, dstate STRING, dcity STRING, fname STRING, 
    lname STRING, name STRING, weight INT, height INT, bats STRING, 
    throws STRING, debut STRING, finalgame STRING, retro STRING, bbref STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION '/user/maria_dev/hivetest/master';

DROP TABLE IF EXISTS bat_data;
CREATE TABLE IF NOT EXISTS bat_data(id STRING, total_hits INT);
INSERT INTO TABLE bat_data
    SELECT b.id as id, sum(b.hits) as total_hits
    FROM batting b
    JOIN (
      SELECT id
      FROM master
      WHERE bmonth=10 and dyear=2011 and bats='R') m
    WHERE m.id=b.id
    GROUP BY b.id;

SELECT b.id
FROM bat_data b,
    (SELECT max(total_hits) as mx
    FROM bat_data) max_ch
WHERE b.total_hits=max_ch.mx;