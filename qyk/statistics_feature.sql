--训练集上不同字段下出现欺诈的次数统计
drop table if EXISTS atec_all_data_feature_100;
create table atec_all_data_feature_100
as select 
	event_id,
	sum(label) over (PARTITION by opposing_id ) as opid_sum_fraud,
    sum(label) over (partition by client_ip) as client_ip_sum_fraud,
    sum(label) over (partition by device_sign) as device_sign_sum_fraud,
    sum(label) over (partition by cert_prov) as cert_prov_sum_fraud,
    sum(label) over (partition by cert_city) as cert_city_sum_fraud,
    sum(label) over (partition by ip_city) as ip_city_sum_fraud,
    sum(label) over (partition by ip_prov) as ip_prov_sum_fraud
	from 
			(SELECT * 
               ,is_fraud AS label 
           FROM atec_train_series 
          WHERE is_fraud >= 0 --训练集
         UNION ALL 
         SELECT * 
               ,0 AS label 
           FROM atec_train_series 
          WHERE is_fraud = - 1 --无标签样本
         UNION ALL 
         SELECT * 
               ,0 AS label 
           FROM atec_test_series --测试集
             ) tmp ;
--统计原始表每行出现null值的次数
drop table if EXISTS atec_all_data_feature_101;
create table atec_all_data_feature_101
	as select 
    event_id,
    (cast((ip_prov is null) as int) + 
     cast((cert_prov is null)as int) +
     cast((card_bin_prov is null)as int) +
     cast((card_mobile_prov is null)as int) +
     cast((card_cert_prov is null)as int) +
     cast((province is null)as int) +
     cast((ip_city is null)as int) +
	 cast((cert_city is null)as int) +   
     cast((card_bin_city is null)as int) +
     cast((card_mobile_city is null)as int) +
     cast((card_cert_city is null)as int) +
     cast((city is null)as int) +
     cast((card_cert_no is null)as int) +
     cast((income_card_cert_no is null)as int) +
     cast((client_ip is null)as int) +
     cast((network is null)as int) +
     cast((device_sign is null)as int) +
     cast((info_1 is null)as int) +
     cast((info_2 is null)as int) +
     cast((is_one_people is null)as int) +
     cast((mobile_oper_platform is null)as int) +
     cast((operation_channel is null)as int) +
     cast((pay_scene is null)as int) +
     cast((income_card_no is null)as int) +
     cast((income_card_mobile is null)as int) +
     cast((income_card_bank_code is null)as int) +
     cast((is_peer_pay is null)as int) +
     cast((ver is null)as int)      
    ) as null_num
    from atec_all_data_series;

--补充的week历史特征
drop table if EXISTS atec_all_data_week_feature_qyk;
create table atec_all_data_week_feature_qyk
	as select 
    event_id,year,month,day,
    case
	when year = 2017 then   weekofyear(ocu_date)-35
    when year = 2018 then  weekofyear(ocu_date)
 	end as week_num,
   	avg(amt) over (partition by user_id,ocu_weekday order by ocu_date asc) as amt_deal_weekday_avg, 
	count(1) over (PARTITION by user_id,ocu_weekday order by ocu_date asc) as freq_deal_weekday,
    count(1) over (partition by  opposing_id,ocu_weekday order by ocu_date asc) as freq_opposing_id_weekday,
    count(1) over (partition by  client_ip,ocu_weekday order by ocu_date asc) as freq_client_ip_weekday,
    count(1) over (partition by  device_sign,ocu_weekday order by ocu_date asc) as freq_device_sign_weekday
   	from atec_all_data_series;

drop table if exists atec_all_data_feature_110;
create table atec_all_data_feature_110
	as select event_id,
    freq_deal_weekday / week_num as freq_deal_weekday_avg,
    freq_opposing_id_weekday / week_num as freq_opposing_id_weekday_avg,
    freq_client_ip_weekday / week_num as freq_client_ip_weekday_avg,
    freq_device_sign_weekday / week_num as freq_device_sign_weekday_avg
    from atec_all_data_week_feature_qyk;

---for test--
--desc atec_all_data_series;
--select * from atec_all_data_feature_110;