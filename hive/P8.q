--Output the birthMonth/birthState combination that produced the worst players
DROP TABLE IF EXISTS batting;
CREATE EXTERNAL TABLE IF NOT EXISTS batting(
    id STRING, yr INT, team STRING, league STRING, games INT, 
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


DROP TABLE IF EXISTS players;
CREATE TABLE players(id STRING, atbat INT, H INT);
INSERT INTO TABLE players
    SELECT id, sum(ab) as atbat, sum(hits) as H
    FROM batting
    GROUP BY id;

DROP TABLE IF EXISTS mo_st;
CREATE TABLE mo_st(most STRING, num_players INT, atbat INT, H INT);
INSERT INTO TABLE mo_st
    SELECT CONCAT(m.bmonth, '/', m.bstate) as most, count(b.id) num_players ,sum(b.atbat) as atbat, sum(b.H) as H
    FROM master m, players b
    WHERE m.id=b.id and m.bstate is not null and m.bmonth is not null
    GROUP BY m.bmonth, m.bstate
    HAVING atbat>100 and num_players>=5;

SELECT most
FROM (
    SELECT most, h/atbat as stat
    FROM mo_st
    ORDER BY stat ASC) t
LIMIT 1;