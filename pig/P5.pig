--Output the playerID of the player who had the most errors in all seasons combined.

field = LOAD 'hdfs:/user/maria_dev/pig/Fielding.csv' USING PigStorage(',');
sub = FILTER field BY $10>0;
field_data = FOREACH field GENERATE $0 AS playerID, (int)$10 AS E:int;

grouped = GROUP field_data BY playerID;

agg_g = FOREACH grouped GENERATE 
    $0 as playerID, 
    SUM(field_data.E) AS tot_err;

ordered = ORDER agg_g BY tot_err DESC;
a = GROUP ordered ALL;
b = FOREACH a GENERATE MAX(ordered.tot_err) AS max_val;
c = FILTER ordered BY tot_err==b.max_val;
d = FOREACH c GENERATE playerID;

DUMP d;