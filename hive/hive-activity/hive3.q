--What player had the most extra base hits during the entire 1980’s (1980 to 1989)?
DROP TABLE IF EXISTS batting;
CREATE EXTERNAL TABLE IF NOT EXISTS batting(
    id STRING, year INT, team STRING, league STRING, games INT, 
    ab INT, runs INT, hits INT, doubles INT, triples INT, homeruns INT, 
    rbi INT, sb INT, cs INT, walks INT, strikeouts INT, ibb INT, 
    hbp INT, sh INT, sf INT, gidp INT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION '/user/maria_dev/hivetest/batting';

DROP TABLE IF EXISTS bat_data;
CREATE TABLE IF NOT EXISTS bat_data(id STRING, tot_extra_bases INT);
INSERT INTO TABLE bat_data
    SELECT id, sum(doubles + triples + homeruns) as tot_extra_bases
    FROM batting
    WHERE year>=1980 and year<=1989
    GROUP BY id;

SELECT b.id
FROM bat_data b,
    (SELECT max(tot_extra_bases) as mx
    FROM bat_data) maxtra
WHERE b.tot_extra_bases=maxtra.mx;