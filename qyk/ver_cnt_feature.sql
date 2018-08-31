--创建统计表
--统计每天pay_scene的出现次数
drop table if EXISTS atec_all_data_feature_210;
create table atec_all_data_feature_210
as select pay_scene,year,month,day, count(1) as pay_scene_num_day
from atec_all_data_series where pay_scene is not null group by pay_scene,year,month,day;
--得到上一天pay_scene出现的次数
drop table if exists atec_all_data_feature_211;
create table atec_all_data_feature_211
as SELECT pay_scene,year, month,day,
lag(pay_scene_num_day,1)  over (PARTITION by pay_scene order by year,month,day) as pay_scene_num_lag1day
from atec_all_data_feature_210;
INSERT OVERWRITE TABLE atec_all_data_feature_211
select
pay_scene,year,month,day,
case 
when pay_scene_num_lag1day is not null then pay_scene_num_lag1day
when pay_scene_num_lag1day is null then 0
end as pay_scene_num_lag1day
from atec_all_data_feature_211;
--得到历史平均出现次数
drop table if exists atec_all_data_feature_212;
create table atec_all_data_feature_212
as select pay_scene,month,day,
avg(pay_scene_num_lag1day) over (PARTITION by pay_scene order by year,month,day) as pay_scene_num_lag1day_avg
from atec_all_data_feature_211;
--合并原始表
drop table if exists atec_all_data_feature_213;
create table atec_all_data_feature_213
as select a.event_id,b.pay_scene_num_lag1day_avg
from atec_all_data_series a left join atec_all_data_feature_212 b
on a.pay_scene = b.pay_scene  and a.month = b.month and a.day = b.day;
--========创建pay_scene小时统计特征=========
drop table if EXISTS atec_all_data_feature_214;
create table atec_all_data_feature_214
as select pay_scene,ocu_date,count(1) as pay_scene_num_1hr
from atec_all_data_series where pay_scene is not null group by pay_scene,ocu_date;

drop table if EXISTS atec_all_data_feature_215;
create table atec_all_data_feature_215
as select pay_scene,ocu_date,pay_scene_num_1hr,
avg(pay_scene_num_1hr) over (PARTITION by pay_scene order by ocu_date) as pay_scene_num_1hr_avg
from atec_all_data_feature_214;
--合并原始表
drop table if EXISTS atec_all_data_feature_216;
create table atec_all_data_feature_216
as select a.event_id,b.pay_scene_num_1hr,b.pay_scene_num_1hr_avg
from atec_all_data_series a left join atec_all_data_feature_215 b 
on a.pay_scene = b.pay_scene and a.ocu_date = b.ocu_date;

--========历史天里的真实平均次数============
--pay_scene的真实平均次数
drop table if EXISTS  atec_all_data_feature_220;
create table atec_all_data_feature_220
as select t.pay_scene,t.year,t.month,t.day,t.day_num,
  sum(t.pay_scene_num_lag1day) over (partition by t.pay_scene order by t.year,t.month,t.day) as pay_scene_lag1day_sum
from (select *, 
      case 
      when month = 9 then  day - 4
      when month = 10 then (day + 26 )
      when month = 11 then (day + 57 )

      when month  = 1 then day -4 
      when month = 2 then day + 27
      when month =3 then (day + 55 )
      end as day_num
      from atec_all_data_feature_211) t;

drop table if EXISTS atec_all_data_feature_221;
create TABLE atec_all_data_feature_221
  as select pay_scene,year,month,day, (pay_scene_lag1day_sum / day_num) as pay_scene_lag1day_avg_real
from atec_all_data_feature_220;      

drop table if EXISTS atec_all_data_feature_222;
create table atec_all_data_feature_222
  as select a.event_id,b.pay_scene_lag1day_avg_real
  from atec_all_data_series a left join atec_all_data_feature_221 b 
  on a.pay_scene = b.pay_scene and a.month =b.month and a.day = b.day;


--========提取pay_scene的小时真实频次特征=======
drop table if EXISTS atec_all_data_feature_223;
create table atec_all_data_feature_223
as select
            t.pay_scene,t.year, t.month,t.day,t.ocu_hr,
            sum(t.pay_scene_freq_deal_1hr) over (PARTITION by t.pay_scene order by t.year,t.month,t.day,t.ocu_hr) as pay_scene_freq_deal_1hr_sum,
        case 
        when t.month = 9 then (t.day -1 - 4)*24 + t.ocu_hr + 1
        when t.month = 10 then (t.day + 30 -1 - 4)*24 + t.ocu_hr+1
        when t.month = 11 then (t.day + 61 -1 - 4)*24 + t.ocu_hr+1

            when t.month = 1 then (t.day - 1 - 4 )* 24 +t.ocu_hr + 1
        when t.month = 2 then (t.day + 31 -1 - 4)*24 + t.ocu_hr+1
        when t.month = 3 then (t.day + 59 - 1 - 4 )*24 + t.ocu_hr+1
        end as hr_num
from (select
      pay_scene,year,month,day,ocu_hr,
      count(1)  as pay_scene_freq_deal_1hr
      from atec_all_data_series where pay_scene is not null group by pay_scene,year,month,day,ocu_hr ) t ;

drop table if EXISTS atec_all_data_feature_224;
create table atec_all_data_feature_224
      as select a.event_id,b.pay_scene_freq_deal_1hr_avg_real
      from atec_all_data_series a 
      left join 
    (select pay_scene,year,month,day,ocu_hr, 
      (pay_scene_freq_deal_1hr_sum / hr_num) as pay_scene_freq_deal_1hr_avg_real
            from atec_all_data_feature_223) b 
      on a.pay_scene=b.pay_scene and a.year = b.year and a.month = b.month and a.day = b.day and a.ocu_hr=b.ocu_hr;
INSERT OVERWRITE TABLE atec_all_data_feature_224
select event_id,
      case 
    when pay_scene_freq_deal_1hr_avg_real is null then 0
    when pay_scene_freq_deal_1hr_avg_real is not null then pay_scene_freq_deal_1hr_avg_real
    end as pay_scene_freq_deal_1hr_avg_real
    from atec_all_data_feature_224;

--合并特征----
drop table if EXISTS atec_all_data_feature_225;
create table atec_all_data_feature_225
as select a.event_id,a.pay_scene_num_lag1day_avg,
  b.pay_scene_num_1hr,
    b.pay_scene_num_1hr_avg,
    c.pay_scene_lag1day_avg_real,
    d.pay_scene_freq_deal_1hr_avg_real
    from atec_all_data_feature_213 a left join atec_all_data_feature_216 b on a.event_id=b.event_id
    left join atec_all_data_feature_222 c on a.event_id = c.event_id
    left join atec_all_data_feature_224 d on a.event_id = d.event_id;
    
--select * from atec_all_data_feature_225;
