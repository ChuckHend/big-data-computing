--Output the team that had the most errors in 2001.

field = LOAD 'hdfs:/user/maria_dev/pig/Fielding.csv' USING PigStorage(',');

field_data = FOREACH field GENERATE $1 AS year, $2 AS teamID, $10 AS err;

yr2011 = FILTER field_data BY (year==2001); 

grouped = GROUP yr2011 by teamID;

summed = FOREACH grouped GENERATE 
    $0 AS teamID, 
    SUM(yr2011.err) AS total_err;

ordered = ORDER summed BY total_err DESC;

a = GROUP ordered ALL;
b = FOREACH a GENERATE MAX(ordered.total_err) AS max_val;
c = FILTER ordered BY total_err==b.max_val;
d = FOREACH c GENERATE teamID;

DUMP d;