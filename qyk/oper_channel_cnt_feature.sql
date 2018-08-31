--创建统计表
--统计每天operation_channel的出现次数
drop table if EXISTS atec_all_data_feature_290;
create table atec_all_data_feature_290
as select operation_channel,year,month,day, count(1) as operation_channel_num_day
from atec_all_data_series where operation_channel is not null group by operation_channel,year,month,day;
--得到上一天operation_channel出现的次数
drop table if exists atec_all_data_feature_291;
create table atec_all_data_feature_291
as SELECT operation_channel,year, month,day,
lag(operation_channel_num_day,1)  over (PARTITION by operation_channel order by year,month,day) as operation_channel_num_lag1day
from atec_all_data_feature_290;
INSERT OVERWRITE TABLE atec_all_data_feature_291
select
operation_channel,year,month,day,
case 
when operation_channel_num_lag1day is not null then operation_channel_num_lag1day
when operation_channel_num_lag1day is null then 0
end as operation_channel_num_lag1day
from atec_all_data_feature_291;
--得到历史平均出现次数
drop table if exists atec_all_data_feature_292;
create table atec_all_data_feature_292
as select operation_channel,month,day,
avg(operation_channel_num_lag1day) over (PARTITION by operation_channel order by year,month,day) as operation_channel_num_lag1day_avg
from atec_all_data_feature_291;
--合并原始表
drop table if exists atec_all_data_feature_293;
create table atec_all_data_feature_293
as select a.event_id,b.operation_channel_num_lag1day_avg
from atec_all_data_series a left join atec_all_data_feature_292 b
on a.operation_channel = b.operation_channel  and a.month = b.month and a.day = b.day;
--========创建operation_channel小时统计特征=========
drop table if EXISTS atec_all_data_feature_294;
create table atec_all_data_feature_294
as select operation_channel,ocu_date,count(1) as operation_channel_num_1hr
from atec_all_data_series where operation_channel is not null group by operation_channel,ocu_date;

drop table if EXISTS atec_all_data_feature_295;
create table atec_all_data_feature_295
as select operation_channel,ocu_date,operation_channel_num_1hr,
avg(operation_channel_num_1hr) over (PARTITION by operation_channel order by ocu_date) as operation_channel_num_1hr_avg
from atec_all_data_feature_294;
--合并原始表
drop table if EXISTS atec_all_data_feature_296;
create table atec_all_data_feature_296
as select a.event_id,b.operation_channel_num_1hr,b.operation_channel_num_1hr_avg
from atec_all_data_series a left join atec_all_data_feature_295 b 
on a.operation_channel = b.operation_channel and a.ocu_date = b.ocu_date;

--========历史天里的真实平均次数============
--operation_channel的真实平均次数
drop table if EXISTS  atec_all_data_feature_300;
create table atec_all_data_feature_300
as select t.operation_channel,t.year,t.month,t.day,t.day_num,
	sum(t.operation_channel_num_lag1day) over (partition by t.operation_channel order by t.year,t.month,t.day) as operation_channel_lag1day_sum
from (select *, 
      case 
      when month = 9 then  day - 4
      when month = 10 then (day + 26 )
      when month = 11 then (day + 57 )

      when month  = 1 then day -4 
      when month = 2 then day + 27
      when month =3 then (day + 55 )
      end as day_num
      from atec_all_data_feature_291) t;

drop table if EXISTS atec_all_data_feature_301;
create TABLE atec_all_data_feature_301
	as select operation_channel,year,month,day, (operation_channel_lag1day_sum / day_num) as operation_channel_lag1day_avg_real
from atec_all_data_feature_300;      

drop table if EXISTS atec_all_data_feature_302;
create table atec_all_data_feature_302
	as select a.event_id,b.operation_channel_lag1day_avg_real
	from atec_all_data_series a left join atec_all_data_feature_301 b 
	on a.operation_channel = b.operation_channel and a.month =b.month and a.day = b.day;


--========提取operation_channel的小时真实频次特征=======
drop table if EXISTS atec_all_data_feature_303;
create table atec_all_data_feature_303
as select
            t.operation_channel,t.year, t.month,t.day,t.ocu_hr,
            sum(t.operation_channel_freq_deal_1hr) over (PARTITION by t.operation_channel order by t.year,t.month,t.day,t.ocu_hr) as operation_channel_freq_deal_1hr_sum,
        case 
        when t.month = 9 then (t.day -1 - 4)*24 + t.ocu_hr + 1
        when t.month = 10 then (t.day + 30 -1 - 4)*24 + t.ocu_hr+1
        when t.month = 11 then (t.day + 61 -1 - 4)*24 + t.ocu_hr+1

            when t.month = 1 then (t.day - 1 - 4 )* 24 +t.ocu_hr + 1
        when t.month = 2 then (t.day + 31 -1 - 4)*24 + t.ocu_hr+1
        when t.month = 3 then (t.day + 59 - 1 - 4 )*24 + t.ocu_hr+1
        end as hr_num
from (select
      operation_channel,year,month,day,ocu_hr,
      count(1)  as operation_channel_freq_deal_1hr
      from atec_all_data_series where operation_channel is not null group by operation_channel,year,month,day,ocu_hr ) t ;

drop table if EXISTS atec_all_data_feature_304;
create table atec_all_data_feature_304
      as select a.event_id,b.operation_channel_freq_deal_1hr_avg_real
      from atec_all_data_series a 
      left join 
    (select operation_channel,year,month,day,ocu_hr, 
      (operation_channel_freq_deal_1hr_sum / hr_num) as operation_channel_freq_deal_1hr_avg_real
            from atec_all_data_feature_303) b 
      on a.operation_channel=b.operation_channel and a.year = b.year and a.month = b.month and a.day = b.day and a.ocu_hr=b.ocu_hr;
INSERT OVERWRITE TABLE atec_all_data_feature_304
select event_id,
      case 
    when operation_channel_freq_deal_1hr_avg_real is null then 0
    when operation_channel_freq_deal_1hr_avg_real is not null then operation_channel_freq_deal_1hr_avg_real
    end as operation_channel_freq_deal_1hr_avg_real
    from atec_all_data_feature_304;

--合并特征----
drop table if EXISTS atec_all_data_feature_305;
create table atec_all_data_feature_305
as select a.event_id,a.operation_channel_num_lag1day_avg,
	b.operation_channel_num_1hr,
    b.operation_channel_num_1hr_avg,
    c.operation_channel_lag1day_avg_real,
    d.operation_channel_freq_deal_1hr_avg_real
    from atec_all_data_feature_293 a left join atec_all_data_feature_296 b on a.event_id=b.event_id
    left join atec_all_data_feature_302 c on a.event_id = c.event_id
    left join atec_all_data_feature_304 d on a.event_id = d.event_id;
    
