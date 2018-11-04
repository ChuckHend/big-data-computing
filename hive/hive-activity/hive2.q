--what player played for the most teams in any single season?
DROP TABLE IF EXISTS batting;
CREATE EXTERNAL TABLE IF NOT EXISTS batting(
    id STRING, year INT, team STRING, league STRING, games INT, 
    ab INT, runs INT, hits INT, doubles INT, triples INT, homeruns INT, 
    rbi INT, sb INT, cs INT, walks INT, strikeouts INT, ibb INT, 
    hbp INT, sh INT, sf INT, gidp INT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION '/user/maria_dev/hivetest/batting';


DROP TABLE IF EXISTS players;
CREATE TABLE IF NOT EXISTS players(id STRING, year int, num_teams INT);
INSERT INTO TABLE players
    SELECT b.id, b.year, count(b.team) as num_teams
    FROM batting b
    GROUP BY b.id, b.year;

SELECT p.id
FROM players p,
    (SELECT max(num_teams) as max_tm
    FROM players) max_tm
WHERE p.num_Teams=max_tm.max_tm;