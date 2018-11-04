--Output the second most common weight.
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
CREATE TABLE IF NOT EXISTS players(weight INT, wcount INT);
INSERT INTO TABLE players
    SELECT weight, count(*) as wcount
    FROM master
    GROUP BY weight
    ORDER BY wcount DESC;

SELECT weight
FROM (
    SELECT weight, RANK() OVER (ORDER BY wcount DESC) as rk
    FROM players) tab
WHERE tab.rk=2;
