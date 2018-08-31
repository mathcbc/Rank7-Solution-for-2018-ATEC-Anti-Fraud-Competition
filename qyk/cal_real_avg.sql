--========历史天里的真实平均次数============
--opposing_id的真实平均次数
drop table if EXISTS  atec_all_data_feature_70;
create table atec_all_data_feature_70
as select t.opposing_id,t.year,t.month,t.day,t.day_num,
  sum(t.opposing_id_num_lag1day) over (partition by t.opposing_id order by t.year,t.month,t.day) as opposing_id_lag1day_sum
from (select *, 
      case 
      when month = 9 then  day - 4
      when month = 10 then (day + 26 )
      when month = 11 then (day + 57 )
      
      when month = 2 then day - 5
      when month =3 then (day + 23 )
      end as day_num
      from atec_all_data_feature_31) t;

drop table if EXISTS atec_all_data_feature_71;
create TABLE atec_all_data_feature_71
  as select opposing_id,year,month,day, (opposing_id_lag1day_sum / day_num) as opposing_id_lag1day_avg_real
from atec_all_data_feature_70;      

drop table if EXISTS atec_all_data_feature_72;
create table atec_all_data_feature_72
  as select a.event_id,b.opposing_id_lag1day_avg_real
  from atec_all_data_series a left join atec_all_data_feature_71 b 
  on a.opposing_id = b.opposing_id and a.month =b.month and a.day = b.day;
    
--client_ip的真实平均次数
drop table if EXISTS atec_all_data_feature_73;
create table atec_all_data_feature_73
as select t.client_ip,t.year,t.month,t.day,t.day_num,
  sum(t.client_ip_num_lag1day) over (partition by t.client_ip order by t.year,t.month,t.day) as client_ip_lag1day_sum
from (select *, 
      case 
      when month = 9 then  day - 4
      when month = 10 then (day + 26 )
      when month = 11 then (day + 57 )

      when month = 2 then day - 5
      when month =3 then (day + 23 )
      end as day_num
      from atec_all_data_feature_35) t;

drop table if EXISTS atec_all_data_feature_74;
create TABLE atec_all_data_feature_74
  as select client_ip,year,month,day, (client_ip_lag1day_sum / day_num) as client_ip_lag1day_avg_real
from atec_all_data_feature_73;

drop table if EXISTS atec_all_data_feature_75;
create table atec_all_data_feature_75
  as select a.event_id,b.client_ip_lag1day_avg_real
  from atec_all_data_series a left join atec_all_data_feature_74 b 
  on a.client_ip = b.client_ip and a.month =b.month and a.day = b.day;
INSERT OVERWRITE TABLE atec_all_data_feature_75
select event_id,
  case 
    when client_ip_lag1day_avg_real is null then 0
    when client_ip_lag1day_avg_real is not null then client_ip_lag1day_avg_real
    end as client_ip_lag1day_avg_real
    from atec_all_data_feature_75;

--device_sign的真实平均次数
drop table if EXISTS atec_all_data_feature_76;
create table atec_all_data_feature_76
as select t.device_sign,t.year,t.month,t.day,t.day_num,
  sum(t.device_sign_num_lag1day) over (partition by t.device_sign order by t.year,t.month,t.day) as device_sign_lag1day_sum
from (select *, 
      case 
      when month = 9 then  day - 4
      when month = 10 then (day + 26 )
      when month = 11 then (day + 57 )

      when month = 2 then day - 5
      when month =3 then (day + 23 )
      end as day_num
      from atec_all_data_feature_39) t;

drop table if EXISTS atec_all_data_feature_77;
create TABLE atec_all_data_feature_77
  as select device_sign,year,month,day, (device_sign_lag1day_sum / day_num) as device_sign_lag1day_avg_real
from atec_all_data_feature_76;      

drop table if EXISTS atec_all_data_feature_78;
create table atec_all_data_feature_78
  as select a.event_id,b.device_sign_lag1day_avg_real
  from atec_all_data_series a left join atec_all_data_feature_77 b 
  on a.device_sign = b.device_sign and a.month =b.month and a.day = b.day;
INSERT OVERWRITE TABLE atec_all_data_feature_78
select event_id,
      case 
    when device_sign_lag1day_avg_real is null then 0
    when device_sign_lag1day_avg_real is not null then device_sign_lag1day_avg_real
    end as device_sign_lag1day_avg_real
    from atec_all_data_feature_78;

--付款方交易频次的真实天平均次数
drop table if EXISTS atec_all_data_feature_79;
create table atec_all_data_feature_79
as select t.user_id,t.year,t.month,t.day,t.day_num,
  sum(t.lag1_freq_deal_sum1day) over (partition by t.user_id order by t.year,t.month,t.day) as user_id_lag1day_sum
from (select *, 
      case 
      when month = 9 then  day - 4
      when month = 10 then (day + 26 )
      when month = 11 then (day + 57 )

      when month = 2 then day - 5
      when month =3 then (day + 23 )
      end as day_num
      from atec_all_data_feature_19) t;

drop table if EXISTS atec_all_data_feature_80;
create TABLE atec_all_data_feature_80
  as select user_id,year,month,day, (user_id_lag1day_sum / day_num) as user_id_lag1day_avg_real
from atec_all_data_feature_79;

drop table if EXISTS atec_all_data_feature_81;
create table atec_all_data_feature_81
  as select a.event_id,b.user_id_lag1day_avg_real
  from atec_all_data_series a left join atec_all_data_feature_80 b 
  on a.user_id = b.user_id and a.month =b.month and a.day = b.day;
--合并特征------
drop table if EXISTS atec_all_data_feature_82;
create table atec_all_data_feature_82
as select a.event_id,a.opposing_id_lag1day_avg_real,
b.client_ip_lag1day_avg_real,c.device_sign_lag1day_avg_real,
d.user_id_lag1day_avg_real 
from atec_all_data_feature_72 a 
join atec_all_data_feature_75 b on a.event_id=b.event_id
join atec_all_data_feature_78 c on a.event_id=c.event_id
join atec_all_data_feature_81 d on a.event_id=d.event_id;

--========提取user_id的小时真实频次特征=======
drop table if EXISTS atec_all_data_feature_83;
create table atec_all_data_feature_83
as select
    t.user_id,t.year, t.month,t.day,t.ocu_hr,
    sum(t.user_id_freq_deal_1hr) over (PARTITION by t.user_id order by t.year,t.month,t.day,t.ocu_hr) as user_id_freq_deal_1hr_sum,
        case 
        when t.month = 9 then (t.day -1 - 4)*24 + t.ocu_hr +1
        when t.month = 10 then (t.day + 30 -1 - 4)*24 + t.ocu_hr + 1
        when t.month = 11 then (t.day + 61 -1 - 4)*24 + t.ocu_hr + 1

--    when t.month = 1 then (t.day - 1 - 4 )* 24 +t.ocu_hr + 1
        when t.month = 2 then (t.day -1 - 5)*24 + t.ocu_hr + 1
        when t.month = 3 then (t.day - 1 + 28 - 5 )*24 + t.ocu_hr + 1
        end as hr_num
from (select
  user_id,year,month,day,ocu_hr,
  count(1)  as user_id_freq_deal_1hr
  from atec_all_data_series group by user_id,year,month,day,ocu_hr) t ;

drop table if EXISTS atec_all_data_feature_84;
create table atec_all_data_feature_84
  as select a.event_id,b.user_id_freq_deal_1hr_avg_real
  from atec_all_data_series a 
  left join 
    (select user_id,year,month,day,ocu_hr, 
      (user_id_freq_deal_1hr_sum / hr_num) as user_id_freq_deal_1hr_avg_real
    from atec_all_data_feature_83) b 
  on a.user_id=b.user_id and a.year = b.year and a.month = b.month and a.day = b.day and a.ocu_hr = b.ocu_hr;

--========提取opposing_id的小时真实频次特征=======
drop table if EXISTS atec_all_data_feature_85;
create table atec_all_data_feature_85
as select
    t.opposing_id,t.year, t.month,t.day,t.ocu_hr,
    sum(t.opposing_id_freq_deal_1hr) over (PARTITION by t.opposing_id order by t.year,t.month,t.day,t.ocu_hr) as opposing_id_freq_deal_1hr_sum,
        case 
        when t.month = 9 then (t.day -1 - 4)*24 + t.ocu_hr + 1
        when t.month = 10 then (t.day + 30 -1 - 4)*24 + t.ocu_hr + 1
        when t.month = 11 then (t.day + 61 -1 - 4)*24 + t.ocu_hr + 1

--    when t.month = 1 then (t.day - 1 - 4 )* 24 +t.ocu_hr + 1
        when t.month = 2 then (t.day -1 - 5)*24 + t.ocu_hr + 1
        when t.month = 3 then (t.day - 1 + 28 - 5 )*24 + t.ocu_hr + 1
        end as hr_num
from (select
  opposing_id,year,month,day,ocu_hr,
  count(1)  as opposing_id_freq_deal_1hr
  from atec_all_data_series group by opposing_id,year,month,day,ocu_hr) t ;

drop table if EXISTS atec_all_data_feature_86;
create table atec_all_data_feature_86
  as select a.event_id,b.opposing_id_freq_deal_1hr_avg_real
  from atec_all_data_series a 
  left join 
    (select opposing_id,year,month,day,ocu_hr, 
      (opposing_id_freq_deal_1hr_sum / hr_num) as opposing_id_freq_deal_1hr_avg_real
    from atec_all_data_feature_85) b 
  on a.opposing_id=b.opposing_id and a.year = b.year and a.month = b.month and a.day = b.day and a.ocu_hr =b.ocu_hr;

--========提取client_ip的小时真实频次特征=======
drop table if EXISTS atec_all_data_feature_87;
create table atec_all_data_feature_87
as select
    t.client_ip,t.year, t.month,t.day,t.ocu_hr,
    sum(t.client_ip_freq_deal_1hr) over (PARTITION by t.client_ip order by t.year,t.month,t.day,t.ocu_hr) as client_ip_freq_deal_1hr_sum,
        case 
        when t.month = 9 then (t.day -1 - 4)*24 + t.ocu_hr + 1
        when t.month = 10 then (t.day + 30 -1 - 4)*24 + t.ocu_hr + 1
        when t.month = 11 then (t.day + 61 -1 - 4)*24 + t.ocu_hr + 1

--    when t.month = 1 then (t.day - 1 - 4 )* 24 +t.ocu_hr + 1
        when t.month = 2 then (t.day -1 - 5)*24 + t.ocu_hr + 1
        when t.month = 3 then (t.day - 1 + 28 - 5 )*24 + t.ocu_hr + 1
        end as hr_num
from (select
  client_ip,year,month,day,ocu_hr,
  count(1)  as client_ip_freq_deal_1hr
  from atec_all_data_series where client_ip is not null group by client_ip,year,month,day,ocu_hr ) t ;

drop table if EXISTS atec_all_data_feature_88;
create table atec_all_data_feature_88
  as select a.event_id,b.client_ip_freq_deal_1hr_avg_real
  from atec_all_data_series a 
  left join 
    (select client_ip,year,month,day,ocu_hr, 
      (client_ip_freq_deal_1hr_sum / hr_num) as client_ip_freq_deal_1hr_avg_real
    from atec_all_data_feature_87) b 
  on a.client_ip=b.client_ip and a.year = b.year and a.month = b.month and a.day = b.day and a.ocu_hr=b.ocu_hr;
INSERT OVERWRITE TABLE atec_all_data_feature_88
select event_id,
  case 
    when client_ip_freq_deal_1hr_avg_real is null then 0
    when client_ip_freq_deal_1hr_avg_real is not null then client_ip_freq_deal_1hr_avg_real
    end as client_ip_freq_deal_1hr_avg_real
    from atec_all_data_feature_88;
    
--========提取device_sign的小时真实频次特征=======
drop table if EXISTS atec_all_data_feature_89;
create table atec_all_data_feature_89
as select
    t.device_sign,t.year, t.month,t.day,t.ocu_hr,
    sum(t.device_sign_freq_deal_1hr) over (PARTITION by t.device_sign order by t.year,t.month,t.day,t.ocu_hr) as device_sign_freq_deal_1hr_sum,
        case 
        when t.month = 9 then (t.day -1 - 4)*24 + t.ocu_hr + 1
        when t.month = 10 then (t.day + 30 -1 - 4)*24 + t.ocu_hr+1
        when t.month = 11 then (t.day + 61 -1 - 4)*24 + t.ocu_hr+1

--    when t.month = 1 then (t.day - 1 - 4 )* 24 +t.ocu_hr + 1
        when t.month = 2 then (t.day -1 - 5)*24 + t.ocu_hr + 1
        when t.month = 3 then (t.day - 1 + 28 - 5 )*24 + t.ocu_hr + 1
        end as hr_num
from (select
  device_sign,year,month,day,ocu_hr,
  count(1)  as device_sign_freq_deal_1hr
  from atec_all_data_series where device_sign is not null group by device_sign,year,month,day,ocu_hr ) t ;

drop table if EXISTS atec_all_data_feature_91;
create table atec_all_data_feature_91
  as select a.event_id,b.device_sign_freq_deal_1hr_avg_real
  from atec_all_data_series a 
  left join 
    (select device_sign,year,month,day,ocu_hr, 
      (device_sign_freq_deal_1hr_sum / hr_num) as device_sign_freq_deal_1hr_avg_real
    from atec_all_data_feature_89) b 
  on a.device_sign=b.device_sign and a.year = b.year and a.month = b.month and a.day = b.day and a.ocu_hr=b.ocu_hr;
INSERT OVERWRITE TABLE atec_all_data_feature_91
select event_id,
  case 
    when device_sign_freq_deal_1hr_avg_real is null then 0
    when device_sign_freq_deal_1hr_avg_real is not null then device_sign_freq_deal_1hr_avg_real
    end as device_sign_freq_deal_1hr_avg_real
    from atec_all_data_feature_91;

--合并特征----
drop table if EXISTS atec_all_data_feature_92;
create table atec_all_data_feature_92
as select a.event_id,a.user_id_freq_deal_1hr_avg_real,
  b.opposing_id_freq_deal_1hr_avg_real,
    c.client_ip_freq_deal_1hr_avg_real,
    d.device_sign_freq_deal_1hr_avg_real
    from atec_all_data_feature_84 a left join atec_all_data_feature_86 b on a.event_id=b.event_id
    left join atec_all_data_feature_88 c on a.event_id=c.event_id
    left join atec_all_data_feature_91 d on a.event_id=d.event_id;

--for test ---
--select count(1) from atec_all_data_feature_92 ;
--select count(1) from atec_all_data_series;