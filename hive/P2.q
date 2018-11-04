--Output the top 3 birth days in MM/DD format
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
CREATE TABLE IF NOT EXISTS players(bday INT, bmonth INT, tot_bdays INT);
INSERT INTO TABLE players
    SELECT m.bday as bday, m.bmonth as bmonth, count(m.id) as tot_bdays
    FROM master m
    WHERE m.bday IS NOT NULL    
    GROUP BY m.bday, m.bmonth
    ORDER BY tot_bdays DESC;

SELECT CONCAT(tab.bmonth, '/', tab.bday) as top_3_bdays
FROM 
    (
    SELECT bday, bmonth, RANK() OVER (ORDER BY tot_bdays DESC) as rk
    FROM players ) tab
WHERE tab.rk<=3;