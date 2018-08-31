--创建统计表
--统计每天opposing_id的出现次数
drop table if EXISTS atec_all_data_feature_30;
create table atec_all_data_feature_30
as select opposing_id,year,month,day, count(1) as opposing_id_num_day
from atec_all_data_series group by opposing_id,year,month,day;
--得到上一天opposing_id出现的次数
drop table if exists atec_all_data_feature_31;
create table atec_all_data_feature_31
as SELECT opposing_id,year, month,day,
lag(opposing_id_num_day,1)  over (PARTITION by opposing_id order by year,month,day) as opposing_id_num_lag1day
from atec_all_data_feature_30;
INSERT OVERWRITE TABLE atec_all_data_feature_31
select
opposing_id,year,month,day,
case 
when opposing_id_num_lag1day is not null then opposing_id_num_lag1day
when opposing_id_num_lag1day is null then 0
end as opposing_id_num_lag1day
from atec_all_data_feature_31;
--得到历史平均出现次数
drop table if exists atec_all_data_feature_32;
create table atec_all_data_feature_32
as select opposing_id,month,day,
avg(opposing_id_num_lag1day) over (PARTITION by opposing_id order by year,month,day) as opposing_id_num_lag1day_avg
from atec_all_data_feature_31;
--合并原始表
drop table if exists atec_all_data_feature_33;
create table atec_all_data_feature_33
as select a.event_id,b.opposing_id_num_lag1day_avg
from atec_all_data_series a left join atec_all_data_feature_32 b
on a.opposing_id = b.opposing_id  and a.month = b.month and a.day = b.day;

--==========创建client_ip统计表============
--统计每天client_ip的出现次数
drop table if EXISTS atec_all_data_feature_34;
create table atec_all_data_feature_34
as select client_ip,year,month,day, count(1) as client_ip_num_day
from atec_all_data_series where client_ip is not null group by client_ip,year,month,day;
--得到上一天client_ip出现的次数
drop table if EXISTS atec_all_data_feature_35;
create table atec_all_data_feature_35
as SELECT client_ip,year,month,day,
lag(client_ip_num_day,1)  over (PARTITION by client_ip order by year,month,day) as client_ip_num_lag1day
from atec_all_data_feature_34;
INSERT OVERWRITE TABLE atec_all_data_feature_35
select
client_ip,year,month,day,
case 
when client_ip_num_lag1day is not null then client_ip_num_lag1day
when client_ip_num_lag1day is null then 0
end as client_ip_num_lag1day
from atec_all_data_feature_35;
--得到历史平均出现次数
drop table if EXISTS atec_all_data_feature_36;
create table atec_all_data_feature_36
as select client_ip,year,month,day,
avg(client_ip_num_lag1day) over (PARTITION by client_ip order by year,month,day) as client_ip_num_lag1day_avg
from atec_all_data_feature_35;
--合并原始表
drop table if EXISTS atec_all_data_feature_37;
create table atec_all_data_feature_37
as select a.event_id,b.client_ip_num_lag1day_avg
from atec_all_data_series a left join atec_all_data_feature_36 b
on a.client_ip = b.client_ip and a.year = b.year and a.month = b.month and a.day = b.day;

--==========创建device_sign统计表============
--统计每天device_sign的出现次数
drop table if EXISTS atec_all_data_feature_38;
create table atec_all_data_feature_38
as select device_sign,year,month,day, count(1) as device_sign_num_day
from atec_all_data_series where device_sign is not null group by device_sign,year,month,day;
--得到上一天device_sign出现的次数
drop table if EXISTS atec_all_data_feature_39;
create table atec_all_data_feature_39
as SELECT device_sign,year,month,day,
lag(device_sign_num_day,1)  over (PARTITION by device_sign order by year,month,day) as device_sign_num_lag1day
from atec_all_data_feature_38;
INSERT OVERWRITE TABLE atec_all_data_feature_39
select
device_sign,year,month,day,
case 
when device_sign_num_lag1day is not null then device_sign_num_lag1day
when device_sign_num_lag1day is null then 0
end as device_sign_num_lag1day
from atec_all_data_feature_39;
--得到历史平均出现次数
drop table if EXISTS atec_all_data_feature_40;
create table atec_all_data_feature_40
as select device_sign,year,month,day,
avg(device_sign_num_lag1day) over (PARTITION by device_sign order by year,month,day) as device_sign_num_lag1day_avg
from atec_all_data_feature_39;
--合并原始表
drop table if EXISTS atec_all_data_feature_41;
create table atec_all_data_feature_41
as select a.event_id,b.device_sign_num_lag1day_avg
from atec_all_data_series a left join atec_all_data_feature_40 b
on a.device_sign = b.device_sign and a.year = b.year and a.month = b.month and a.day = b.day;

--将三个特征合并
drop table if exists atec_all_data_feature_42;
create table atec_all_data_feature_42
as select a.event_id,a.opposing_id_num_lag1day_avg,b.client_ip_num_lag1day_avg,
c.device_sign_num_lag1day_avg from atec_all_data_feature_33 a join atec_all_data_feature_37 b
on a.event_id=b.event_id join atec_all_data_feature_41 c on a.event_id = c.event_id;

--========创建opposing_id小时统计特征=========
drop table if EXISTS atec_all_data_feature_43;
create table atec_all_data_feature_43
as select opposing_id,ocu_date,count(1) as opposing_id_num_1hr
from atec_all_data_series group by opposing_id,ocu_date;

drop table if EXISTS atec_all_data_feature_44;
create table atec_all_data_feature_44
as select opposing_id,ocu_date,opposing_id_num_1hr,
avg(opposing_id_num_1hr) over (PARTITION by opposing_id order by ocu_date) as opposing_id_num_1hr_avg
from atec_all_data_feature_43;
--合并原始表
drop table if EXISTS atec_all_data_feature_45;
create table atec_all_data_feature_45
as select a.event_id,b.opposing_id_num_1hr,b.opposing_id_num_1hr_avg
from atec_all_data_series a left join atec_all_data_feature_44 b 
on a.opposing_id = b.opposing_id and a.ocu_date = b.ocu_date;

--========创建client_ip小时统计特征=========
drop table if EXISTS atec_all_data_feature_46;
create table atec_all_data_feature_46
as select client_ip,ocu_date,count(1) as client_ip_num_1hr
from atec_all_data_series where client_ip is not null group by client_ip,ocu_date;

drop table if EXISTS  atec_all_data_feature_47;
create table atec_all_data_feature_47
as select client_ip,ocu_date,client_ip_num_1hr,
avg(client_ip_num_1hr) over (PARTITION by client_ip order by ocu_date) as client_ip_num_1hr_avg
from atec_all_data_feature_46;
--合并原始表
drop table if EXISTS atec_all_data_feature_48;
create table atec_all_data_feature_48
as select a.event_id,b.client_ip_num_1hr,b.client_ip_num_1hr_avg
from atec_all_data_series a left join atec_all_data_feature_47 b 
on a.client_ip = b.client_ip and a.ocu_date = b.ocu_date;
INSERT OVERWRITE TABLE atec_all_data_feature_48
select event_id,
case 
when client_ip_num_1hr is not null then client_ip_num_1hr
when client_ip_num_1hr is null then 0
end as client_ip_num_1hr,
case
when client_ip_num_1hr_avg is not null then client_ip_num_1hr_avg
when client_ip_num_1hr_avg is null then 0
end as client_ip_num_1hr_avg
from atec_all_data_feature_48;

--========创建device_sign小时统计特征=========
drop table if EXISTS atec_all_data_feature_49;
create table atec_all_data_feature_49
as select device_sign,ocu_date,count(1) as device_sign_num_1hr
from atec_all_data_series where device_sign is not null group by device_sign,ocu_date;

drop table if EXISTS  atec_all_data_feature_50;
create table atec_all_data_feature_50
as select device_sign,ocu_date,device_sign_num_1hr,
avg(device_sign_num_1hr) over (PARTITION by device_sign order by ocu_date) as device_sign_num_1hr_avg
from atec_all_data_feature_49;
--合并原始表
drop table if EXISTS atec_all_data_feature_51;
create table atec_all_data_feature_51
as select a.event_id,b.device_sign_num_1hr,b.device_sign_num_1hr_avg
from atec_all_data_series a left join atec_all_data_feature_50 b 
on a.device_sign = b.device_sign and a.ocu_date = b.ocu_date;
INSERT OVERWRITE TABLE atec_all_data_feature_51
select event_id,
case 
when device_sign_num_1hr is not null then device_sign_num_1hr
when device_sign_num_1hr is null then 0
end as device_sign_num_1hr,
case
when device_sign_num_1hr_avg is not null then device_sign_num_1hr_avg
when device_sign_num_1hr_avg is null then 0
end as device_sign_num_1hr_avg
from atec_all_data_feature_51;

--合并小时特征
drop table if EXISTS atec_all_data_feature_52;
create table atec_all_data_feature_52
as select a.event_id,a.opposing_id_num_1hr_avg,
b.client_ip_num_1hr_avg,c.device_sign_num_1hr_avg
from atec_all_data_feature_45 a join atec_all_data_feature_48 b on a.event_id = b.event_id
join atec_all_data_feature_51 c on a.event_id = c.event_id;

-----------===============================================

--消费金额连续相同次数统计
drop table if EXISTS atec_all_data_feature_53;
create table atec_all_data_feature_53
as select event_id, row_number() over (PARTITION  by user_id,amt order by ocu_date) as same_amt_num
from atec_all_data_series;
--此交易往前一天的交易频次的特征
-- select t1.event_id, count(1) as freq_deal_last1day
-- from atec_all_data_series t1 
-- left outer join (select user_id,ocu_date from atec_all_data_series) t2
-- on t1.user_id = t2.user_id
-- where t1.user_id != 1767924
-- and t1.ocu_date >= t2.ocu_date
-- and t1.ocu_date < dateadd(t2.ocu_date,1,'day')
-- group by t1.event_id;

--====上一天的交易频次特征======
drop table if EXISTS atec_all_data_feature_54;
create table atec_all_data_feature_54
as select a.event_id,b.lag1_freq_deal_sum1day 
from atec_all_data_series a 
left join atec_all_data_feature_19 b
on a.user_id = b.user_id and a.month = b.month and a.day = b.day;
--===上一天opposing_id出现的频次===
drop table if EXISTS atec_all_data_feature_55;
create table atec_all_data_feature_55
as select  /* + mapjoin(b) */ a.event_id,b.opposing_id_num_lag1day
from atec_all_data_series a left join 
atec_all_data_feature_31 b 
on a.opposing_id = b.opposing_id and a.month = b.month and a.day = b.day;
--==上一天client_ip出现的频次====
drop table if EXISTS atec_all_data_feature_56;
create table atec_all_data_feature_56
as select /* + mapjoin(b) */ a.event_id,b.client_ip_num_lag1day
from atec_all_data_series a left join 
atec_all_data_feature_35 b
on a.client_ip = b.client_ip and a.month = b.month and a.day = b.day;
--=======上一天device_sign出现的频次====
drop table if EXISTS atec_all_data_feature_57;
create table atec_all_data_feature_57
as select /* + mapjoin(b) */ a.event_id,b.device_sign_num_lag1day 
from atec_all_data_series a left join 
atec_all_data_feature_39 b 
on a.device_sign = b.device_sign and a.month = b.month and a.day = b.day;
--===上一天card_cert_no出现的频次=====
--统计每天card_cert_no的出现次数
drop table if EXISTS atec_all_data_feature_58;
create table atec_all_data_feature_58
as select card_cert_no,year,month,day, count(1) as card_cert_no_num_day
from atec_all_data_series where card_cert_no is not null group by card_cert_no,year,month,day;
--得到上一天card_cert_no出现的次数
drop table if EXISTS atec_all_data_feature_59;
create table atec_all_data_feature_59
as SELECT card_cert_no,year,month,day,
lag(card_cert_no_num_day,1)  over (PARTITION by card_cert_no order by year,month,day) as card_cert_no_num_lag1day
from atec_all_data_feature_58;
INSERT OVERWRITE TABLE atec_all_data_feature_59
select
card_cert_no,year,month,day,
case 
when card_cert_no_num_lag1day is not null then card_cert_no_num_lag1day
when card_cert_no_num_lag1day is null then 0
end as card_cert_no_num_lag1day
from atec_all_data_feature_59;

drop table if EXISTS atec_all_data_feature_60;
create table atec_all_data_feature_60
as select /* + mapjoin(b) */ a.event_id,b.card_cert_no_num_lag1day 
from atec_all_data_series a left join 
atec_all_data_feature_59 b 
on a.card_cert_no = b.card_cert_no and a.month = b.month and a.day = b.day;
INSERT OVERWRITE TABLE atec_all_data_feature_60
select event_id,
case 
when card_cert_no_num_lag1day is null then 0
when card_cert_no_num_lag1day is not null then card_cert_no_num_lag1day
end as card_cert_no_num_lag1day
from atec_all_data_feature_60;
--=====合并上一天的频次特征
drop table if EXISTS atec_all_data_feature_61;
create table atec_all_data_feature_61
as select a.event_id,
a.lag1_freq_deal_sum1day,--上一天交易频次特征
b.opposing_id_num_lag1day,--上一天opposing_id出现的频次
c.client_ip_num_lag1day,--上一天client_ip出现的频次
d.device_sign_num_lag1day,--上一天device_sign出现的频次
e.card_cert_no_num_lag1day--上一天card_cert_no出现的频次
from atec_all_data_feature_54 a join atec_all_data_feature_55 b on a.event_id = b.event_id
join atec_all_data_feature_56 c on a.event_id = c.event_id
join atec_all_data_feature_57 d on a.event_id = d.event_id
join atec_all_data_feature_60 e on a.event_id = e.event_id;

--===user_id,opposing_id上一天同时出现的次数=======
--创建统计表
--统计每天user_id,opposing_id的出现次数
drop table if EXISTS atec_all_data_feature_65;
create table atec_all_data_feature_65
as select user_id,opposing_id,year,month,day, count(1) as user_id_opposing_id_same_day
from atec_all_data_series group by user_id,opposing_id,year,month,day;
--得到上一天user_id,opposing_id出现的次数
drop table if exists atec_all_data_feature_66;
create table atec_all_data_feature_66
as SELECT user_id,opposing_id,year, month,day,
lag(user_id_opposing_id_same_day,1)  over (PARTITION by user_id,opposing_id order by year,month,day) as user_id_opposing_id_same_lag1day
from atec_all_data_feature_65;
INSERT OVERWRITE TABLE atec_all_data_feature_66
select
user_id,opposing_id,year,month,day,
case 
when user_id_opposing_id_same_lag1day is not null then user_id_opposing_id_same_lag1day
when user_id_opposing_id_same_lag1day is null then 0
end as user_id_opposing_id_same_lag1day
from atec_all_data_feature_66;

----for test------
