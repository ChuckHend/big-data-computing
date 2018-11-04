--Output the top 3 birth days in MM/DD format
master = LOAD 'hdfs:/user/maria_dev/pig/Master.csv' USING PigStorage(',');
real = FILTER master BY $2>0;
master_data = FOREACH real GENERATE $0 AS playerID, $2 AS month, $3 AS day;

--group by person
mast_g = GROUP master_data by (month, day);

-- agg sum on AB
mast_ct = FOREACH mast_g GENERATE 
    $0 as month_day, 
    COUNT(master_data.playerID) AS ct;

ordered = ORDER mast_ct by ct DESC;

ord3 = LIMIT ordered 3;

flat = FOREACH ord3 GENERATE FLATTEN(month_day);

result = FOREACH flat GENERATE CONCAT($0, '/', $1);

DUMP result;