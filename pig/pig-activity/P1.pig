--Who was the heaviest player to hit more than 5 triples (3B) in 2005?
batters = LOAD 'hdfs:/user/maria_dev/pigtest/batting/Batting.csv' using PigStorage(',');
master = LOAD 'hdfs:/user/maria_dev/pigtest/master/Master.csv' using PigStorage(',');

-- select the data we need
bat_data = FOREACH batters GENERATE $0 AS playerID, $1 AS yearID, $9 AS triples;
pers_data = FOREACH master GENERATE $0 AS playerID, $16 AS weight;
--filter batters
g_5_trips = FILTER bat_data BY triples>5 and yearID==2005;
--join with names
names_stats = JOIN pers_data BY playerID, g_5_trips BY playerID;

nice_ns = FOREACH names_stats GENERATE $0 as playerID, $1 as weight;

a = GROUP nice_ns ALL;
b = FOREACH a GENERATE MAX(nice_ns.weight) as max_val;
c = FILTER nice_ns BY weight == b.max_val;
heavies = FOREACH c GENERATE $0;

DUMP heavies;