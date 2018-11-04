--Of the right-handed batters who were born in October and died in 2011, which one had the most hits in his career?
batters = LOAD 'hdfs:/user/maria_dev/pigtest/batting/Batting.csv' USING PigStorage(',');
master = LOAD 'hdfs:/user/maria_dev/pigtest/master/Master.csv' USING PigStorage(',');

bat_data = FOREACH batters GENERATE $0 AS playerID, (int)$7 AS hits:int;
master_data = FOREACH master GENERATE $0 AS playerID, (int)$2 AS birthMo:int, (int)$7 AS deathYr:int, $18 AS bats;

mast = FILTER master_data BY (birthMo==10 AND deathYr==2011 AND bats=='R');

--join
joined = JOIN mast BY playerID, bat_data BY playerID;

--group by person
joined_g = GROUP joined BY $0;

--calc sum hits for each player
player_hits = FOREACH joined_g GENERATE $0 AS playerID, SUM(joined.hits) AS tot;

--get max hit nums
a = GROUP player_hits ALL;
b = FOREACH a GENERATE MAX(player_hits.tot) AS max_val;
c = FILTER player_hits BY tot==b.max_val;
d = FOREACH c GENERATE $0;
DUMP d;