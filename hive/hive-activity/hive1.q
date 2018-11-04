--Who was the heaviest player to hit more than 5 triples (3B) in 2005?
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

DROP TABLE IF EXISTS players;
CREATE TABLE IF NOT EXISTS players(id STRING, weight int);
INSERT INTO TABLE players
    SELECT m.id, m.weight
    FROM master m, batting b
    WHERE m.id=b.id and b.year=2005 and b.triples>5;

SELECT p.id
FROM players p,
    (SELECT max(weight) as max_wt
    FROM players) max_w
WHERE p.weight=max_w.max_wt;