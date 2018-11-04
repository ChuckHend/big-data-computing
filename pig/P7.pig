--dump top 5 sum(doubles and trips) for city/state combo
b = LOAD 'hdfs:/user/maria_dev/pig/Batting.csv' USING PigStorage(',');
m = LOAD 'hdfs:/user/maria_dev/pig/Master.csv' USING PigStorage(',');

bd = FOREACH b GENERATE $0 AS playerID, $8 AS doub, $9 AS trip;
bat_data = FILTER bd BY (doub >=0 AND trip >=0);
mas_data = FOREACH m GENERATE $0 AS playerID, $5 AS state, $6 AS city;

all_data = JOIN bat_data by playerID, mas_data by playerID;

all_d = FOREACH all_data GENERATE city, state, doub, trip;

all_g = GROUP all_d BY (mas_data::city, mas_data::state);

all_agg = FOREACH all_g GENERATE group, 
    SUM(all_d.bat_data::doub) as dubs, 
    SUM(all_d.bat_data::trip) as trips;

all_sum = FOREACH all_agg GENERATE group, (dubs + trips) as total;

sorted = ORDER all_sum BY total DESC;

result = LIMIT sorted 5;

ans = FOREACH result GENERATE FLATTEN(group);

out = FOREACH ans GENERATE CONCAT($0, '/', $1);

DUMP out;