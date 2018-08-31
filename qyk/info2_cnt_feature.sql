--创建统计表
--统计每天info_2的出现次数
drop table if EXISTS atec_all_data_feature_270;
create table atec_all_data_feature_270
as select info_2,year,month,day, count(1) as info_2_num_day
from atec_all_data_series where info_2 is not null group by info_2,year,month,day;
--得到上一天info_2出现的次数
drop table if exists atec_all_data_feature_271;
create table atec_all_data_feature_271
as SELECT info_2,year, month,day,
lag(info_2_num_day,1)  over (PARTITION by info_2 order by year,month,day) as info_2_num_lag1day
from atec_all_data_feature_270;
INSERT OVERWRITE TABLE atec_all_data_feature_271
select
info_2,year,month,day,
case 
when info_2_num_lag1day is not null then info_2_num_lag1day
when info_2_num_lag1day is null then 0
end as info_2_num_lag1day
from atec_all_data_feature_271;
--得到历史平均出现次数
drop table if exists atec_all_data_feature_272;
create table atec_all_data_feature_272
as select info_2,month,day,
avg(info_2_num_lag1day) over (PARTITION by info_2 order by year,month,day) as info_2_num_lag1day_avg
from atec_all_data_feature_271;
--合并原始表
drop table if exists atec_all_data_feature_273;
create table atec_all_data_feature_273
as select a.event_id,b.info_2_num_lag1day_avg
from atec_all_data_series a left join atec_all_data_feature_272 b
on a.info_2 = b.info_2  and a.month = b.month and a.day = b.day;
--========创建info_2小时统计特征=========
drop table if EXISTS atec_all_data_feature_274;
create table atec_all_data_feature_274
as select info_2,ocu_date,count(1) as info_2_num_1hr
from atec_all_data_series where info_2 is not null group by info_2,ocu_date;

drop table if EXISTS atec_all_data_feature_275;
create table atec_all_data_feature_275
as select info_2,ocu_date,info_2_num_1hr,
avg(info_2_num_1hr) over (PARTITION by info_2 order by ocu_date) as info_2_num_1hr_avg
from atec_all_data_feature_274;
--合并原始表
drop table if EXISTS atec_all_data_feature_276;
create table atec_all_data_feature_276
as select a.event_id,b.info_2_num_1hr,b.info_2_num_1hr_avg
from atec_all_data_series a left join atec_all_data_feature_275 b 
on a.info_2 = b.info_2 and a.ocu_date = b.ocu_date;

--========历史天里的真实平均次数============
--info_2的真实平均次数
drop table if EXISTS  atec_all_data_feature_280;
create table atec_all_data_feature_280
as select t.info_2,t.year,t.month,t.day,t.day_num,
  sum(t.info_2_num_lag1day) over (partition by t.info_2 order by t.year,t.month,t.day) as info_2_lag1day_sum
from (select *, 
      case 
      when month = 9 then  day - 4
      when month = 10 then (day + 26 )
      when month = 11 then (day + 57 )

      when month  = 1 then day -4 
      when month = 2 then day + 27
      when month =3 then (day + 55 )
      end as day_num
      from atec_all_data_feature_271) t;

drop table if EXISTS atec_all_data_feature_281;
create TABLE atec_all_data_feature_281
  as select info_2,year,month,day, (info_2_lag1day_sum / day_num) as info_2_lag1day_avg_real
from atec_all_data_feature_280;      

drop table if EXISTS atec_all_data_feature_282;
create table atec_all_data_feature_282
  as select a.event_id,b.info_2_lag1day_avg_real
  from atec_all_data_series a left join atec_all_data_feature_281 b 
  on a.info_2 = b.info_2 and a.month =b.month and a.day = b.day;


--========提取info_2的小时真实频次特征=======
drop table if EXISTS atec_all_data_feature_283;
create table atec_all_data_feature_283
as select
            t.info_2,t.year, t.month,t.day,t.ocu_hr,
            sum(t.info_2_freq_deal_1hr) over (PARTITION by t.info_2 order by t.year,t.month,t.day,t.ocu_hr) as info_2_freq_deal_1hr_sum,
        case 
        when t.month = 9 then (t.day -1 - 4)*24 + t.ocu_hr + 1
        when t.month = 10 then (t.day + 30 -1 - 4)*24 + t.ocu_hr+1
        when t.month = 11 then (t.day + 61 -1 - 4)*24 + t.ocu_hr+1

            when t.month = 1 then (t.day - 1 - 4 )* 24 +t.ocu_hr + 1
        when t.month = 2 then (t.day + 31 -1 - 4)*24 + t.ocu_hr+1
        when t.month = 3 then (t.day + 59 - 1 - 4 )*24 + t.ocu_hr+1
        end as hr_num
from (select
      info_2,year,month,day,ocu_hr,
      count(1)  as info_2_freq_deal_1hr
      from atec_all_data_series where info_2 is not null group by info_2,year,month,day,ocu_hr ) t ;

drop table if EXISTS atec_all_data_feature_284;
create table atec_all_data_feature_284
      as select a.event_id,b.info_2_freq_deal_1hr_avg_real
      from atec_all_data_series a 
      left join 
    (select info_2,year,month,day,ocu_hr, 
      (info_2_freq_deal_1hr_sum / hr_num) as info_2_freq_deal_1hr_avg_real
            from atec_all_data_feature_283) b 
      on a.info_2=b.info_2 and a.year = b.year and a.month = b.month and a.day = b.day and a.ocu_hr=b.ocu_hr;
INSERT OVERWRITE TABLE atec_all_data_feature_284
select event_id,
      case 
    when info_2_freq_deal_1hr_avg_real is null then 0
    when info_2_freq_deal_1hr_avg_real is not null then info_2_freq_deal_1hr_avg_real
    end as info_2_freq_deal_1hr_avg_real
    from atec_all_data_feature_284;

--合并特征----
drop table if EXISTS atec_all_data_feature_285;
create table atec_all_data_feature_285
as select a.event_id,a.info_2_num_lag1day_avg,
  b.info_2_num_1hr,
    b.info_2_num_1hr_avg,
    c.info_2_lag1day_avg_real,
    d.info_2_freq_deal_1hr_avg_real
    from atec_all_data_feature_273 a left join atec_all_data_feature_276 b on a.event_id=b.event_id
    left join atec_all_data_feature_282 c on a.event_id = c.event_id
    left join atec_all_data_feature_284 d on a.event_id = d.event_id;
    

select * from atec_all_data_feature_285 ;

