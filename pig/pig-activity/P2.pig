--return name of player who played on most teams in single season

batters = LOAD 'hdfs:/user/maria_dev/pigtest/batting/Batting.csv' USING PigStorage(',');
-- subset
myBatts = FOREACH batters GENERATE $0 AS playerID, $1 AS yearID;
--group by person, year, team
bat_yr_tm_gp = GROUP myBatts by ($0, $1);
--agg with count
team_ct = FOREACH bat_yr_tm_gp GENERATE group, COUNT(myBatts.$0) as num_teams;
--get max num teams
a = GROUP team_ct ALL;
b = FOREACH a GENERATE MAX(team_ct.num_teams) as max_val;
c = FILTER team_ct BY num_teams == b.max_val;
d = FOREACH c GENERATE FLATTEN($0);
max_players = FOREACH d GENERATE FLATTEN($0);
DUMP max_players;
