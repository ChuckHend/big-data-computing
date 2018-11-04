--What player had the most extra base hits during the entire 1980’s (1980 to 1989)?
batters = LOAD 'hdfs:/user/maria_dev/pigtest/batting/Batting.csv' USING PigStorage(',');
-- subset
bat_data = FOREACH batters GENERATE $0 AS playerID, $1 AS yearID, $8 AS two, $9 AS three, $10 as HR;

-- sub for the year range 1980-1989 inclusive
bat_years = FILTER bat_data BY yearID>1979 and yearID<1990;

--create group
bat_group = GROUP bat_years BY (playerID);

--do colsum
bat_colsum = FOREACH bat_group GENERATE $0, SUM(bat_years.two) as two, SUM(bat_years.three) as three, SUM(bat_years.HR) as HR;

--do rowsum
bat_rowsum = FOREACH bat_colsum GENERATE $0, ($1 + $2 + $3) as extraBases;

--max max hits
a = GROUP bat_rowsum ALL;
b = FOREACH a GENERATE MAX(bat_rowsum.extraBases) as max_val;
c = FILTER bat_rowsum BY extraBases==b.max_val;
d = FOREACH c GENERATE FLATTEN($0);

DUMP d;
