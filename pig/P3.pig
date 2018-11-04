--Output the second most common weight.

master = LOAD 'hdfs:/user/maria_dev/pig/Master.csv' USING PigStorage(',');

sub = FOREACH master GENERATE $0 as playerID, $16 as weight;
real_g = GROUP sub BY weight;

ct = FOREACH real_g GENERATE 
    $0, 
    COUNT(sub.playerID) as freq;

ordered = ORDER ct BY freq DESC;

top2 = LIMIT ordered 2;

a = GROUP top2 ALL;
b = FOREACH a GENERATE MIN(top2.freq) AS min_val;
c = FILTER ordered BY freq==b.min_val;
d = FOREACH c GENERATE $0 as weight;

DUMP d;