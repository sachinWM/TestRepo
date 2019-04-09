
-- ### Pulling traced sales ####

--Store list: vn501e0.new_stores

DROP  TABLE vn501e0.Tre_st_traced_neigh_str_Sales;
CREATE TABLE vn501e0.Tre_st_traced_neigh_str_Sales stored as ORC as 
SELECT ogp_store, launch_date, neigh_str,

SUM(total_auth_amount) AS Total_Sales,
SUM(CASE WHEN channel  = 'STORE' THEN total_auth_amount ELSE 0 END ) AS BnM_Sales,
SUM(CASE WHEN channel  = 'OG' THEN total_auth_amount ELSE 0 END ) AS OG_Sales

FROM 
(select a.ogp_store, a.neigh_str, b.launch_week, b.launch_date from 
(select ogp_store, neigh_str from vn501e0.cross_shop_v3 group by ogp_store, neigh_str) a
left join vn501e0.new_stores b
on a.ogp_store=b.delivery_store) a

JOIN gcia_dotcom.sol_v1 b
ON a.neigh_str = b.store_nbr
WHERE b.visit_date < a.Launch_Date  and DATEDIFF(a.Launch_Date ,b.visit_date ) <=180 
AND  b.store_nbr IS NOT NULL AND b.individual_id IS NOT NULL
AND (b.channel = 'STORE' OR  b.channel = 'OG')
GROUP BY ogp_store,launch_date,neigh_str;

hive -e 'set hive.cli.print.header = true;set hive.resultset.use.unique.column.names=false;
select * from vn501e0.Tre_st_traced_neigh_str_Sales;'>Tre_st_traced_neigh_str_Sales.txt;

--QC
SELECT * FROM vn5006i.Tre_vin_CS_All_traced_neigh_str_2017_Sales 
WHERE ogp_store = 4 and neigh_str = 6960 
;

4       2017-03-29      6960    6800513.909999996       6800513.909999996      0.0

-- QC (checking if the data was present in omni channel table
SELECT store_nbr, channel, sum(total_auth_amount) sales , count(distinct Visit_date) days
FROM gcia_dotcom.omnichannel_sol_v1 
WHERE (channel = 'STORE' OR  channel = 'OG' )
and Visit_date BETWEEN  '2012-03-17' AND '2011-09-19'
and store_nbr =  '5647'
AND individual_id IS NOT NULL 
group by store_nbr, channel
;






-- ### Pulling OG traced sales ####

--Store list: vn501e0.new_stores

DROP  TABLE vn501e0.Tre_st_traced_neigh_str_Sales_OG;
CREATE TABLE vn501e0.Tre_st_traced_neigh_str_Sales_OG stored as ORC as 
SELECT ogp_store, launch_date, neigh_str,

SUM(CASE WHEN channel  = 'OG' THEN total_auth_amount ELSE 0 END ) AS OG_Sales

FROM 
(select a.ogp_store, a.neigh_str, b.launch_week, b.launch_date from 
(select ogp_store, neigh_str from vn501e0.cross_shop_v3 group by ogp_store, neigh_str) a
left join vn501e0.new_stores b
on a.ogp_store=b.delivery_store) a

JOIN 
(select a.*, b.delivery_store  
from gcia_dotcom.sol_v1 a 
left join (select distinct order_id,delivery_store from vn501ea.sam_2) as b 
on a.group_order_nbr = b.order_id
where a.store_nbr IS NOT NULL AND a.individual_id IS NOT NULL
AND a.channel = 'OG' ) b
ON a.neigh_str = b.delivery_store
WHERE b.visit_date < a.Launch_Date  and DATEDIFF(a.Launch_Date ,b.visit_date ) <=180 
GROUP BY ogp_store,launch_date,neigh_str;

hive -e 'set hive.cli.print.header = true;set hive.resultset.use.unique.column.names=false;
select * from vn501e0.Tre_st_traced_neigh_str_Sales;'>Tre_st_traced_neigh_str_Sales.txt;





DROP  TABLE vn501e0.Tre_st_traced_neigh_str_Sales_Str1353;
CREATE TABLE vn501e0.Tre_st_traced_neigh_str_Sales_Str1353 stored as ORC as 
SELECT ogp_store, launch_date, neigh_str,

SUM(total_auth_amount) AS Total_Sales,
SUM(CASE WHEN channel  = 'STORE' THEN total_auth_amount ELSE 0 END ) AS BnM_Sales,
SUM(CASE WHEN channel  = 'OG' THEN total_auth_amount ELSE 0 END ) AS OG_Sales

FROM 
(select a.ogp_store, a.neigh_str, b.launch_week, b.launch_date from 
(select ogp_store, neigh_str from vn501e0.cross_shop_v3 where ogp_store= '1353' and neigh_str= '9894' group by ogp_store, neigh_str) a
left join vn501e0.new_stores b
on a.ogp_store=b.delivery_store) a

JOIN gcia_dotcom.sol_v1 b
ON a.neigh_str = b.store_nbr
WHERE b.visit_date < a.Launch_Date  and DATEDIFF(a.Launch_Date ,b.visit_date ) <=180 
AND  b.store_nbr = '9894' AND b.individual_id IS NOT NULL
AND (b.channel = 'STORE' OR  b.channel = 'OG')
GROUP BY ogp_store,launch_date,neigh_str;

1353, 9894



































