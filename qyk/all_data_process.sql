--feature extract
--历史交易金额特征提取
drop table if exists atec_all_data_feature_1;
create table atec_all_data_feature_1
as select 
	event_id,
	avg(amt) over (partition by user_id order by ocu_date) as amt_avg,
    max(amt) over (partition by user_id order by ocu_date) as amt_max,
    min(amt) over (partition by user_id order by ocu_date) as amt_min
    from atec_all_data_series;

--计算历史交易金额和当次消费相减的特征
drop table if EXISTS atec_all_data_feature_6;
create table atec_all_data_feature_6
as select 
a.event_id,
(a.amt - b.amt_avg) as amt_subtract_avg,--当前金额减去历史交易平均
(a.amt - b.amt_max) as amt_subtract_max,--当前金额减去历史金额最大
(a.amt - b.amt_min) as amt_substact_min --当前金额减去历史金额最小
from atec_all_data_series a join atec_all_data_feature_1 b 
on a.event_id = b.event_id;

--过去1天历史交易金额特征
-- create table atec_all_data_feature_2
-- as select 
-- event_id,
-- sum(amt) as lastday_amt_sum, --lastday交易金额之和
-- avg(amt) as lastday_amt_avg, --lastday交易金额平均
-- max(amt) as lastday_amt_max, --lastday交易金额最大
-- min(amt) as lastday_amt_min -- lastday交易金额最小
-- from (select t2.amt,t1.event_id from atec_all_data_series t1 left outer join atec_all_data_series t2
-- on t1.user_id = t2.user_id 
-- where ( (t1.ocu_date >=t2.ocu_date) and (t1.ocu_date <= dateadd(t2.ocu_date,1,'day') ) ) ) tmp
-- group by event_id;

--提取支付账户user_id和收款账户opposing_id相同的特征
drop table if EXISTS atec_all_data_feature_3;
create table atec_all_data_feature_3
as select 
event_id,
count(1) as receive_num, --该支付账户本次交易前收款次数
sum(amt) as receive_amt_sum,--该支付账户本次交易前收款总额
avg(amt) as receive_amt_avg,--该支付账户本次交易前收款平均额度
max(amt) as receive_amt_max--该支付账户本次交易前收款最大额度
from (select t2.amt,t1.event_id from atec_all_data_series t1 inner join atec_all_data_series t2
on (t1.user_id = t2.opposing_id ) where (t1.ocu_date >=t2.ocu_date) ) tmp
group by event_id;
--合并驱动表
drop table if EXISTS atec_all_data_feature_4;
create table atec_all_data_feature_4
as select a.event_id,b.receive_num,b.receive_amt_sum,b.receive_amt_max,b.receive_amt_avg
from atec_all_data_series a left join atec_all_data_feature_3 b on a.event_id = b.event_id;

drop table if EXISTS atec_all_data_feature_5;
create table atec_all_data_feature_5
	as select event_id,
    case
    when receive_num is not null then receive_num
    when receive_num is null then 0
    end as receive_num,
    case
    when receive_amt_sum is not null then receive_amt_sum
    when receive_amt_sum is null then 0
    end as receive_amt_sum,
    case
    when receive_amt_max is not null then receive_amt_max
    when receive_amt_max is null then 0
    end as receive_amt_max,
    case
    when receive_amt_avg is not null then receive_amt_avg
    when receive_amt_avg is null then 0
    end as receive_amt_avg
    from atec_all_data_feature_4;

--提取过去所有历史交易的频次特征
drop table if EXISTS atec_all_data_feature_7;
	create table atec_all_data_feature_7
	as select 
	event_id,
	count(1)  over (PARTITION by user_id order by ocu_date asc) as freq_deal_sum
from atec_all_data_series;

--提取当前一天截止当笔交易的频次特征
drop table if EXISTS atec_all_data_feature_8;
create table atec_all_data_feature_8
	as select 
	event_id,
	count(1)  over (PARTITION by user_id,month,day order by ocu_date asc) as freq_deal_1day
from atec_all_data_series;
--提取当前一小时交易的频次特征
drop table if exists atec_all_data_feature_9;
create table atec_all_data_feature_9
	as select
	event_id,
	count(1) over (partition by user_id,month,day,ocu_hr) as freq_deal_1hr
from atec_all_data_series;
--合并频次特征
drop table if EXISTS atec_all_data_feature_10;
create table atec_all_data_feature_10
	as select
	a.event_id,a.freq_deal_sum,b.freq_deal_1day,c.freq_deal_1hr
	from atec_all_data_feature_7 a join atec_all_data_feature_8 b on a.event_id = b.event_id
	join atec_all_data_feature_9 c on a.event_id = c.event_id;

--提取截止当前交易周中的第几天累计历史交易平均额度/频次
drop table if EXISTS atec_all_data_feature_11;
CREATE table atec_all_data_feature_11
	as select 
	event_id,
	avg(amt) over (partition by user_id,ocu_weekday order by ocu_date) as amt_deal_weekday_avg, 
	count(1) over (PARTITION by user_id,ocu_weekday order by ocu_date) as freq_deal_weekday
from atec_all_data_series;

--提取使用同一ip的不同设备、证件、账号的特征
------未执行---------
-- drop table if EXISTS atec_all_data_feature_12;
-- create table atec_all_data_feature_12 as
-- 	select 
-- 	t1.event_id,
-- 	count(DISTINCT t2.device_sign) as ip_device_cnt
-- 	from atec_all_data_series t1 left outer join atec_all_data_series t2
-- 	on t1.client_ip = t2.client_ip 
-- 	where t1.ocu_date >=t2.ocu_date
-- 	group by t1.event_id;


--for test------------------------------------
--select * from atec_all_data_feature_12;
