-- ip与收款方的历史频次特征

-- ip在此次交易前出现的次数
drop TABLE if EXISTS ip_appear_cnt_cbc;
create TABLE ip_appear_cnt_cbc as
SELECT 
event_id,
count(event_id) over(PARTITION by client_ip ORDER BY ocu_date asc) as ip_appear_cnt
from atec_all_data_series
;

-- ip一小时内出现的次数
drop TABLE if EXISTS ip_1hr_cnt_cbc;
create table ip_1hr_cnt_cbc
as select
event_id,
count(*) over (partition by client_ip,month,day,ocu_hr) as ip_1hr_cnt
from atec_all_data_series;

-- 收款方一小时内的收款次数（此交易前）
drop TABLE if EXISTS payee_1hr_get_cnt_cbc;
create table payee_1hr_get_cnt_cbc
as select
event_id,
count(*) over (partition by opposing_id,month,day,ocu_hr) as payee_1hr_get_cnt
from atec_all_data_series;

drop table if EXISTS extra_cnt_featurce_cbc;
CREATE TABLE extra_cnt_featurce_cbc
as 
SELECT 
t1.event_id
,ip_appear_cnt
,ip_1hr_cnt
,payee_1hr_get_cnt
from
ip_appear_cnt_cbc t1
left OUTER JOIN 
ip_1hr_cnt_cbc t2
on t1.event_id=t2.event_id
left outer join
payee_1hr_get_cnt_cbc t3
on t1.event_id=t3.event_id
;