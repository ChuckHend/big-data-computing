b = LOAD 'hdfs:/user/maria_dev/pig/Batting.csv' USING PigStorage(',');
m = LOAD 'hdfs:/user/maria_dev/pig/Master.csv' USING PigStorage(',');

mast = FILTER m BY ($2>0 AND $5 is not null);
mast_data = FOREACH mast GENERATE 
    $0 AS playerID, 
    $2 AS birthMonth, 
    $5 AS birthState;
    
bat_data = FOREACH b GENERATE 
    $0 AS playerID, 
    $5 AS AB, 
    $7 AS H;

all_d = JOIN mast_data BY playerID, bat_data BY playerID;
all_data = FOREACH all_d GENERATE 
    $0 AS playerID, 
    birthMonth, 
    birthState, 
    H, 
    AB;

all_g = GROUP all_data BY (playerID, mast_data::birthMonth, mast_data::birthState);

all_agg = FOREACH all_g GENERATE 
    FLATTEN(group), 
    SUM(all_data.bat_data::H) as hits,
    SUM(all_data.bat_data::AB) as bats;
    
all_mo_st = GROUP all_agg BY (group::mast_data::birthMonth,group::mast_data::birthState);

all_mo_st_agg = FOREACH all_mo_st GENERATE
    group,
    SUM(all_agg.hits) AS hits,
    SUM(all_agg.bats) AS bats,
    COUNT(all_agg.group::playerID) as people;
    
all_filt = FILTER all_mo_st_agg BY (people>=5 AND bats>=100);

all_calc = FOREACH all_filt GENERATE 
    group, 
    (hits / bats) as BA;

sorted = ORDER all_calc BY BA ASC;

result = LIMIT sorted 1;

ans = FOREACH result GENERATE FLATTEN(group);

out = FOREACH ans GENERATE CONCAT($0, '/', $1);
DUMP out;