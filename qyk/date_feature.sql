drop table if exists atec_all_data_feature_201;
create table atec_all_data_feature_201
  as select event_id,
    case 
    when month = 9 and (day = 30 or day=29 ) then 1
    when month = 10 and (day = 30 or day = 31 ) then 1
--    when month = 1 and (day = 30 or day =31 ) then 1
    when month = 2 and (day = 27 or day =28 ) then 1
    else 0
    end as month_end_day,
    case
    when ocu_date >= '2017-10-01 00:00:00' and ocu_date < '2017-10-09 00:00:00' then 1
--    when ocu_date >= '2018-01-01 00:00:00' and ocu_date <= '2018-01-02 00:00:00' then 1
    when ocu_date >= '2018-02-15 00:00:00' and ocu_date <= '2018-02-21 00:00:00' then 1
    else 0
    end as is_holiday,
    case
    when month = 10 and day =1 then 1/8*2*3.142
    when month = 10 and day =2 then 2/8*2*3.142
    when month = 10 and day =3 then 3/8*2*3.142
    when month = 10 and day =4 then 4/8*2*3.142
    when month = 10 and day =5 then 5/8*2*3.142
    when month = 10 and day =6 then 6/8*2*3.142
    when month = 10 and day =7 then 7/8*2*3.142
    when month = 10 and day =8 then 8/8*2*3.142
    when month = 2 and day = 15 then 1/7*3*3.142
    when month = 2 and day = 16 then 2/7*3*3.142
    when month = 2 and day = 17 then 3/7*3*3.142
    when month = 2 and day = 18 then 4/7*3*3.142
    when month = 2 and day = 19 then 5/7*3*3.142
    when month = 2 and day = 20 then 6/7*3*3.142
    when month = 2 and day = 21 then 7/7*3*3.142
    else 0
    end as holiday_series
    from atec_all_data_series;



--for test--
select count(1) from result08200934 where score >0.5;
select * from atec_all_data_feature_201;