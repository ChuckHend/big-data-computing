--Output the playerID’s of the top 3 players inclusive 2005 through 2009 with max:
--(number of hits/number of at bats – (number of errors/ number of games)

f = LOAD 'hdfs:/user/maria_dev/pig/Fielding.csv' USING PigStorage(',');
filt = FILTER f by (int)$8 is not null;
filt_field = FILTER filt BY ($1 >= 2005 AND $1 <= 2009);

b = LOAD 'hdfs:/user/maria_dev/pig/Batting.csv' USING PigStorage(',');
batters = FILTER b BY ($1 >= 2005 AND $1 <= 2009);

bat_data = FOREACH batters GENERATE (bytearray)$0 AS playerID:bytearray, (int)$5 AS AB:float, (int)$7 AS H:float;
field_data = FOREACH filt_field GENERATE $0 AS playerID, (int)$5 AS G:float, (int)$10 AS E:float;

bat_g = GROUP bat_data BY playerID;
field_g = GROUP field_data BY playerID;

bat_agg = FOREACH bat_g GENERATE 
    $0 as playerID, 
    SUM(bat_data.H) as hit, 
    SUM(bat_data.AB) AS atbats;
field_agg = FOREACH field_g GENERATE 
    $0 as playerID, 
    SUM(field_data.G) as games, 
    SUM(field_data.E) as err;

bat_stat = FOREACH bat_agg GENERATE 
    playerID, 
    atbats, 
    (hit / atbats) as bat_stat;
min_bat_stat = FILTER bat_stat BY atbats >=40;
field_stat = FOREACH field_agg GENERATE 
    playerID, 
    games, 
    (err/games) as field_stat;
    
min_field_stat = FILTER field_stat BY games>=20;
combine_stat = JOIN min_bat_stat BY playerID, min_field_stat by playerID;

test_stat = FOREACH combine_stat GENERATE 
    $0 AS playerID, 
    (bat_stat - field_stat) as t_stat;

ordered = ORDER test_stat BY t_stat DESC;

result = LIMIT ordered 3;

ans = FOREACH result GENERATE FLATTEN($0);

DUMP ans;
