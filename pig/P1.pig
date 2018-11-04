--Output the birth city of the player who had the most at bats (AB) in his career.
batters = LOAD 'hdfs:/user/maria_dev/pig/Batting.csv' USING PigStorage(',');
master = LOAD 'hdfs:/user/maria_dev/pig/Master.csv' USING PigStorage(',');

bat_data = FOREACH batters GENERATE $0 AS playerID, (int)$5 AS AB:int;
master_data = FOREACH master GENERATE $0 AS playerID, $6 AS birthCity;

--group by person
bat_g = GROUP bat_data BY $0;

-- agg sum on AB
player_ab = FOREACH bat_g GENERATE 
    $0 as playerID, 
    SUM(bat_data.AB) as total_AB;

--get max hit nums
a = GROUP player_ab ALL;
b = FOREACH a GENERATE MAX(player_ab.total_AB) AS max_val;
c = FILTER player_ab BY total_AB==b.max_val;
d = FOREACH c GENERATE  $0 as playerID, $1 as career_AB;

--join
joined = JOIN d BY $0, master_data BY $0;

result = FOREACH joined GENERATE birthCity;

DUMP result;