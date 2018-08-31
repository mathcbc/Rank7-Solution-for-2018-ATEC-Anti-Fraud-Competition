--创建统计表
--统计每天info_1的出现次数
drop table if EXISTS atec_all_data_feature_250;
create table atec_all_data_feature_250
as select info_1,year,month,day, count(1) as info_1_num_day
from atec_all_data_series where info_1 is not null group by info_1,year,month,day;
--得到上一天info_1出现的次数
drop table if exists atec_all_data_feature_251;
create table atec_all_data_feature_251
as SELECT info_1,year, month,day,
lag(info_1_num_day,1)  over (PARTITION by info_1 order by year,month,day) as info_1_num_lag1day
from atec_all_data_feature_250;
INSERT OVERWRITE TABLE atec_all_data_feature_251
select
info_1,year,month,day,
case 
when info_1_num_lag1day is not null then info_1_num_lag1day
when info_1_num_lag1day is null then 0
end as info_1_num_lag1day
from atec_all_data_feature_251;
--得到历史平均出现次数
drop table if exists atec_all_data_feature_252;
create table atec_all_data_feature_252
as select info_1,month,day,
avg(info_1_num_lag1day) over (PARTITION by info_1 order by year,month,day) as info_1_num_lag1day_avg
from atec_all_data_feature_251;
--合并原始表
drop table if exists atec_all_data_feature_253;
create table atec_all_data_feature_253
as select a.event_id,b.info_1_num_lag1day_avg
from atec_all_data_series a left join atec_all_data_feature_252 b
on a.info_1 = b.info_1  and a.month = b.month and a.day = b.day;
--========创建info_1小时统计特征=========
drop table if EXISTS atec_all_data_feature_254;
create table atec_all_data_feature_254
as select info_1,ocu_date,count(1) as info_1_num_1hr
from atec_all_data_series where info_1 is not null group by info_1,ocu_date;

drop table if EXISTS atec_all_data_feature_255;
create table atec_all_data_feature_255
as select info_1,ocu_date,info_1_num_1hr,
avg(info_1_num_1hr) over (PARTITION by info_1 order by ocu_date) as info_1_num_1hr_avg
from atec_all_data_feature_254;
--合并原始表
drop table if EXISTS atec_all_data_feature_256;
create table atec_all_data_feature_256
as select a.event_id,b.info_1_num_1hr,b.info_1_num_1hr_avg
from atec_all_data_series a left join atec_all_data_feature_255 b 
on a.info_1 = b.info_1 and a.ocu_date = b.ocu_date;

--========历史天里的真实平均次数============
--info_1的真实平均次数
drop table if EXISTS  atec_all_data_feature_260;
create table atec_all_data_feature_260
as select t.info_1,t.year,t.month,t.day,t.day_num,
  sum(t.info_1_num_lag1day) over (partition by t.info_1 order by t.year,t.month,t.day) as info_1_lag1day_sum
from (select *, 
      case 
      when month = 9 then  day - 4
      when month = 10 then (day + 26 )
      when month = 11 then (day + 57 )

      when month  = 1 then day -4 
      when month = 2 then day + 27
      when month =3 then (day + 55 )
      end as day_num
      from atec_all_data_feature_251) t;

drop table if EXISTS atec_all_data_feature_261;
create TABLE atec_all_data_feature_261
  as select info_1,year,month,day, (info_1_lag1day_sum / day_num) as info_1_lag1day_avg_real
from atec_all_data_feature_260;      

drop table if EXISTS atec_all_data_feature_262;
create table atec_all_data_feature_262
  as select a.event_id,b.info_1_lag1day_avg_real
  from atec_all_data_series a left join atec_all_data_feature_261 b 
  on a.info_1 = b.info_1 and a.month =b.month and a.day = b.day;


--========提取info_1的小时真实频次特征=======
drop table if EXISTS atec_all_data_feature_263;
create table atec_all_data_feature_263
as select
            t.info_1,t.year, t.month,t.day,t.ocu_hr,
            sum(t.info_1_freq_deal_1hr) over (PARTITION by t.info_1 order by t.year,t.month,t.day,t.ocu_hr) as info_1_freq_deal_1hr_sum,
        case 
        when t.month = 9 then (t.day -1 - 4)*24 + t.ocu_hr + 1
        when t.month = 10 then (t.day + 30 -1 - 4)*24 + t.ocu_hr+1
        when t.month = 11 then (t.day + 61 -1 - 4)*24 + t.ocu_hr+1

            when t.month = 1 then (t.day - 1 - 4 )* 24 +t.ocu_hr + 1
        when t.month = 2 then (t.day + 31 -1 - 4)*24 + t.ocu_hr+1
        when t.month = 3 then (t.day + 59 - 1 - 4 )*24 + t.ocu_hr+1
        end as hr_num
from (select
      info_1,year,month,day,ocu_hr,
      count(1)  as info_1_freq_deal_1hr
      from atec_all_data_series where info_1 is not null group by info_1,year,month,day,ocu_hr ) t ;

drop table if EXISTS atec_all_data_feature_264;
create table atec_all_data_feature_264
      as select a.event_id,b.info_1_freq_deal_1hr_avg_real
      from atec_all_data_series a 
      left join 
    (select info_1,year,month,day,ocu_hr, 
      (info_1_freq_deal_1hr_sum / hr_num) as info_1_freq_deal_1hr_avg_real
            from atec_all_data_feature_263) b 
      on a.info_1=b.info_1 and a.year = b.year and a.month = b.month and a.day = b.day and a.ocu_hr=b.ocu_hr;
INSERT OVERWRITE TABLE atec_all_data_feature_264
select event_id,
      case 
    when info_1_freq_deal_1hr_avg_real is null then 0
    when info_1_freq_deal_1hr_avg_real is not null then info_1_freq_deal_1hr_avg_real
    end as info_1_freq_deal_1hr_avg_real
    from atec_all_data_feature_264;

--合并特征----
drop table if EXISTS atec_all_data_feature_265;
create table atec_all_data_feature_265
as select a.event_id,a.info_1_num_lag1day_avg,
  b.info_1_num_1hr,
    b.info_1_num_1hr_avg,
    c.info_1_lag1day_avg_real,
    d.info_1_freq_deal_1hr_avg_real
    from atec_all_data_feature_253 a left join atec_all_data_feature_256 b on a.event_id=b.event_id
    left join atec_all_data_feature_262 c on a.event_id = c.event_id
    left join atec_all_data_feature_264 d on a.event_id = d.event_id;
    


