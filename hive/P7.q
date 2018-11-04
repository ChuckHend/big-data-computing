--dump top 5 sum(doubles and trips) for birth city/state combo
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

DROP TABLE IF EXISTS city_state;
CREATE EXTERNAL TABLE city_state(cityState STRING, dubs INT, trips INT);
INSERT INTO TABLE city_state
    SELECT CONCAT(m.bcity, '/', m.bstate) as cityState, sum(b.doubles) as dubs, sum(b.triples) as trips
    FROM master m, batting b
    WHERE m.id=b.id and m.bcity!='' and m.bstate!=''
    GROUP BY m.bcity, m.bstate;

SELECT t.cityState
FROM (
    SELECT DISTINCT cityState, (dubs+trips) as comb
    FROM city_state
    ORDER BY comb DESC) t
WHERE t.comb is not null
LIMIT 5;

