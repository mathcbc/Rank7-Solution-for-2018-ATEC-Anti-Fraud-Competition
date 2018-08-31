--计算金额的峰度/偏度
DROP TABLE IF EXISTS atec_all_data_feature_13
;


CREATE TABLE atec_all_data_feature_13 AS
  SELECT event_id 
       ,SUM(amt) OVER ( PARTITION BY user_id ORDER BY ocu_date )         AS rx 
       ,SUM(POW(amt ,2)) OVER ( PARTITION BY user_id ORDER BY ocu_date ) AS rx2 
       ,SUM(POW(amt ,3)) OVER ( PARTITION BY user_id ORDER BY ocu_date ) AS rx3 
       ,SUM(POW(amt ,4)) OVER ( PARTITION BY user_id ORDER BY ocu_date ) AS rx4 
       ,COUNT(1) OVER ( PARTITION BY user_id ORDER BY ocu_date )         AS rn 
       ,STDDEV(amt) OVER ( PARTITION BY user_id ORDER BY ocu_date )      AS stdv 
       ,AVG(amt) OVER ( PARTITION BY user_id ORDER BY ocu_date )         AS av 
   FROM atec_all_data_series
;

drop table if EXISTS atec_all_data_feature_14;
create table atec_all_data_feature_14
as select
event_id,
rx as rx_amt,--历史交易金额之和
rx2 as rx2_amt,--历史交易金额平方之和
stdv as stdv_amt,--历史交易金额标准差
case 
when rn <= 2 then  0
when stdv = 0 then 0
when (rn >2 and stdv != 0) then (rx3 - 3*rx2*av + 3*rx*av*av - rn*av*av*av)/ (stdv*stdv*stdv) * rn / (rn-1) / (rn-2)
end AS skewness_amt,--历史交易金额的偏度
case 
when rn <=3 then 0
when stdv = 0 then 0
when (rn>3 and stdv != 0) then  (rx4 - 4*rx3*av + 6*rx2*av*av - 4*rx*av*av*av + rn*av*av*av*av)/ (stdv*stdv*stdv*stdv) * rn * (rn+1) / (rn-1) / (rn-2) / (rn-3)
   - 3.0 * (rn-1) * (rn-1) / (rn-2) / (rn-3) 
end AS kurtosis_amt--历史交易金额的峰度
from atec_all_data_feature_13;

--============计算一小时频次的偏度和峰度=============
--feature15,16,17,23,24
drop table atec_all_data_feature_15;
create table atec_all_data_feature_15
as SELECT 
a.event_id,a.user_id,a.ocu_date,b.freq_deal_1hr
from atec_all_data_series a join atec_all_data_feature_9 b on a.event_id = b.event_id;
--创建统计表
drop table if EXISTS atec_all_data_feature_16;
create table atec_all_data_feature_16
as select user_id,ocu_date,max(freq_deal_1hr) as freq_deal_1hr
from atec_all_data_feature_15 group by user_id,ocu_date,freq_deal_1hr;

drop table if EXISTS atec_all_data_feature_17;
create table atec_all_data_feature_17
as select 
user_id,ocu_date,
sum(freq_deal_1hr)  over (PARTITION by user_id order by ocu_date) as rx,
sum(pow(freq_deal_1hr,2) ) over (PARTITION by user_id order by ocu_date) as rx2,
sum(pow(freq_deal_1hr,3)) over (PARTITION by user_id order by ocu_date) as rx3,
sum(pow(freq_deal_1hr,4)) over (PARTITION by user_id order by ocu_date) as rx4,
count(1) over (PARTITION by user_id order by ocu_date) as rn,
stddev(freq_deal_1hr) over (PARTITION by user_id order by ocu_date) as stdv,
avg(freq_deal_1hr) over (PARTITION by user_id order by ocu_date) as av
from atec_all_data_feature_16;

drop table if EXISTS atec_all_data_feature_23;
create table atec_all_data_feature_23
as select
user_id,ocu_date,
rx as rx_freq_deal_1hr,--这一小时及之前的小时交易频次之和
rx2 as rx2_freq_deal_1hr,--这一小时及之前的小时交易频次平方之和
stdv as stdv_freq_deal_1hr,--这一小时及之前的小时交易频次标准差
av as avg_freq_deal_1hr,--这一小时及之前的小时交易频次均值
case 
when rn <= 2 then  0
when stdv = 0 then 0
when (rn >2 and stdv != 0) then (rx3 - 3*rx2*av + 3*rx*av*av - rn*av*av*av)/ (stdv*stdv*stdv) * rn / (rn-1) / (rn-2)
end AS skewness_freq_1hr,--这一小时及之前的小时交易频次偏度
case 
when rn <=3 then 0
when stdv = 0 then 0
when (rn>3 and stdv != 0) then  (rx4 - 4*rx3*av + 6*rx2*av*av - 4*rx*av*av*av + rn*av*av*av*av)/ (stdv*stdv*stdv*stdv) * rn * (rn+1) / (rn-1) / (rn-2) / (rn-3)
   - 3.0 * (rn-1) * (rn-1) / (rn-2) / (rn-3) 
end AS kurtosis_freq_1hr--这一小时及之前的小时交易频次峰度
from atec_all_data_feature_17;
--合并得到最终特征表
drop table if EXISTS atec_all_data_feature_24;
create table atec_all_data_feature_24
  as SELECT a.event_id,b.rx_freq_deal_1hr,b.rx2_freq_deal_1hr,
  b.stdv_freq_deal_1hr,b.avg_freq_deal_1hr,b.skewness_freq_1hr,b.kurtosis_freq_1hr
  from atec_all_data_series a left join atec_all_data_feature_23 b
  on a.user_id = b.user_id and a.ocu_date = b.ocu_date;


--===============一天频次的偏度和峰度=================
--创建一天频次的统计特征
drop table if EXISTS atec_all_data_feature_18;
create table atec_all_data_feature_18
as select 
user_id,year,month,day,
count(1)  as freq_deal_sum1day
from atec_all_data_series group  by user_id,year,month,day;
--创建统计上一天频次的特征
drop table if exists atec_all_data_feature_19;
create table atec_all_data_feature_19
as select
user_id,year,month,day,
lag(freq_deal_sum1day,1) over (PARTITION by user_id order by year,month,day asc) as lag1_freq_deal_sum1day
from atec_all_data_feature_18;
INSERT OVERWRITE TABLE  atec_all_data_feature_19
select 
user_id,year,month,day,
case 
when lag1_freq_deal_sum1day is not null then lag1_freq_deal_sum1day
when lag1_freq_deal_sum1day is null then 0
end as lag1_freq_deal_sum1day
from  atec_all_data_feature_19;
--创建统计表
drop table if exists atec_all_data_feature_20;
create table atec_all_data_feature_20
as select 
user_id,year,month,day,
sum(lag1_freq_deal_sum1day)  over (PARTITION by user_id order by year,month,day) as rx,
sum(pow(lag1_freq_deal_sum1day,2) ) over (PARTITION by user_id order by year,month,day) as rx2,
sum(pow(lag1_freq_deal_sum1day,3)) over (PARTITION by user_id order by year,month,day) as rx3,
sum(pow(lag1_freq_deal_sum1day,4)) over (PARTITION by user_id order by year,month,day) as rx4,
count(1) over (PARTITION by user_id order by year,month,day) as rn,
stddev(lag1_freq_deal_sum1day) over (PARTITION by user_id order by year,month,day) as stdv,
avg(lag1_freq_deal_sum1day) over (PARTITION by user_id order by year,month,day) as av
from atec_all_data_feature_19;
--统计偏度峰度
drop table if EXISTS atec_all_data_feature_21;
create table atec_all_data_feature_21
as select
user_id,month,day,
rx as rx_lag1_freq_deal_sum1day,--上一天之前的交易频次之和
rx2 as rx2_lag1_freq_deal_sum1day,--上一天之前的交易频次平方之和
stdv as stdv_lag1_freq_deal_sum1day,--上一天之前的交易频次平方差
av as avg_lag1_freq_deal_sum1day,--上一天之前的交易频次平均
case 
when rn <= 2 then  0
when stdv = 0 then 0
when (rn >2 and stdv != 0) then (rx3 - 3*rx2*av + 3*rx*av*av - rn*av*av*av)/ (stdv*stdv*stdv) * rn / (rn-1) / (rn-2)
end AS skewness_freq_sum1day,--上一天之前的交易频次偏度
case 
when rn <=3 then 0
when stdv = 0 then 0
when (rn>3 and stdv != 0) then  (rx4 - 4*rx3*av + 6*rx2*av*av - 4*rx*av*av*av + rn*av*av*av*av)/ (stdv*stdv*stdv*stdv) * rn * (rn+1) / (rn-1) / (rn-2) / (rn-3)
   - 3.0 * (rn-1) * (rn-1) / (rn-2) / (rn-3) 
end AS kurtosis_freq_sum1day--上一天之前的交易频次峰度
from atec_all_data_feature_20;
--和原始表合并得到最终特征表
drop table if EXISTS atec_all_data_feature_22;
create table atec_all_data_feature_22
  as select a.event_id,b.rx_lag1_freq_deal_sum1day,b.rx2_lag1_freq_deal_sum1day,
  b.stdv_lag1_freq_deal_sum1day,b.avg_lag1_freq_deal_sum1day,
  b.skewness_freq_sum1day,b.kurtosis_freq_sum1day
  from atec_all_data_series a left join atec_all_data_feature_21 b
  on a.user_id = b.user_id and a.month = b.month and a.day = b.day;
    
-----时间循环特征
DROP TABLE IF EXISTS atec_all_data_feature_25
;


CREATE TABLE atec_all_data_feature_25 AS
  SELECT event_id 
       ,SIN(ocu_hr / 24 * 2 * 3.1416)     AS hour_circu 
       ,--小时循环特征
        SIN(ocu_weekday / 7 * 2 * 3.1416) AS weekday_circu --星期循环特征
   FROM atec_all_data_series
;

---过去几次消费的特征
drop table if exists atec_all_data_feature_26;
CREATE TABLE atec_all_data_feature_26 AS
  SELECT event_id 
       ,MAX(amt) OVER ( PARTITION BY user_id ORDER BY ocu_date ROWS 5 PRECEDING )  AS prev5_amt_max 
       ,--过去5笔交易金额的最大值
        AVG(amt) OVER ( PARTITION BY user_id ORDER BY ocu_date ROWS 5 PRECEDING )  AS prev5_amt_avg 
       ,--过去5笔交易金额的平均值
        MAX(amt) OVER ( PARTITION BY user_id ORDER BY ocu_date ROWS 7 PRECEDING )  AS prev7_amt_max 
       ,--过去7笔交易金额的最大值
        AVG(amt) OVER ( PARTITION BY user_id ORDER BY ocu_date ROWS 7 PRECEDING )  AS prev7_amt_avg 
       ,--过去7笔交易金额的平均值
        MAX(amt) OVER ( PARTITION BY user_id ORDER BY ocu_date ROWS 10 PRECEDING ) AS prev10_amt_max 
       ,--过去10笔交易金额的最大值
        AVG(amt) OVER ( PARTITION BY user_id ORDER BY ocu_date ROWS 10 PRECEDING ) AS prev10_amt_avg --过去10笔交易金额的平均值
   FROM atec_all_data_series
;


--消费时间间隔特征
DROP TABLE IF EXISTS atec_all_data_feature_27
;
CREATE TABLE atec_all_data_feature_27 AS
  SELECT event_id 
       ,user_id 
       ,ocu_date 
       ,LAG(ocu_date ,1) OVER ( PARTITION BY user_id ORDER BY ocu_date ) AS lag1_date 
   FROM atec_all_data_series
;

DROP TABLE IF EXISTS atec_all_data_feature_28
;
CREATE TABLE atec_all_data_feature_28 AS
  SELECT event_id 
       ,( UNIX_TIMESTAMP(ocu_date)-UNIX_TIMESTAMP(lag1_date) 
        ) / 3600 AS time_interval1 --本次交易和上次交易的时间间隔
   FROM atec_all_data_feature_27
;

----for test ----------------------
--select min(skewness_freq_sum1day),min(kurtosis_freq_sum1day) from atec_all_data_feature_22 ;
--select min(amt_subtract_max) from atec_all_data_feature_6;
--select * from atec_all_data_feature_22 limit 10;
