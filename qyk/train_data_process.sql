--‘上一次’的数据特征提取
drop table if EXISTS atec_train_data_feature_1;
create table atec_train_data_feature_1
as select
	event_id,user_id,ocu_date,ocu_hr,
	amt,client_ip,network,device_sign,mobile_oper_platform,
	operation_channel,pay_scene,income_card_bank_code,
	ip_prov,ip_city,ver,opposing_id,cert_prov,cert_city,
	lag(amt,1) over (partition by user_id order by ocu_date asc) as prev1amt, --上一次消费金额
	lag(client_ip,1) over (partition by user_id order by ocu_date asc) as prev1client_ip, --上一次用户IP
	lag(network,1) over (partition by user_id order by ocu_date asc) as prev1network,--上一次网络类型
	lag(device_sign,1) over (partition by user_id order by ocu_date asc) as prev1device_sign, --上一次设备ID
	lag(mobile_oper_platform,1) over (partition by user_id order by ocu_date asc) as prev1mobile_plat, --上一次手机平台
	lag(operation_channel,1) over (partition by user_id order by ocu_date asc) as prev1oper_channel,--上一次支付方式
	lag(pay_scene,1) over (partition by user_id order by ocu_date asc) as prev1pay_scene,--上一次支付场景
	lag(income_card_bank_code) over (partition by user_id order by ocu_date asc) as prev1income_bank,--上一次收入银行代码
	lag(ip_prov) over (partition by user_id order by ocu_date asc) as prev1ip_prov,--上一次IP省
	lag(ip_city) over (partition by user_id order by ocu_date asc) as prev1ip_city,--上一次IP市
	lag(ver) over (partition by user_id order by ocu_date asc) as prev1version,--上一次版本号
	lag(opposing_id) over (partition by user_id order by ocu_date asc) as prev1opposing_id,--上一次对方虚拟用户ID
	lag(cert_prov) over (partition by user_id order by ocu_date asc) as prev1cert_prov,--上一次证件省
	lag(cert_city) over (partition by user_id order by ocu_date asc) as prev1cert_city--上一次证件省
	from atec_train_series;
-- 判断此次交易和上一次交易是否相同
drop table if EXISTS atec_train_data_feature_2;
create table atec_train_data_feature_2
as select event_id,
prev1amt, prev1client_ip, prev1network,prev1device_sign,
prev1mobile_plat,prev1oper_channel, prev1pay_scene, prev1income_bank,
cast((client_ip <> prev1client_ip) as int ) as is_ip_the_same, --本次和上次IP是否相同
cast( (network <> prev1network) as int ) as is_network_same, --本次和上次网络类型是否相同
cast((device_sign <> prev1device_sign) as int) as is_device_sign_same, --本次和上次设备ID是否相同
cast((mobile_oper_platform <> prev1mobile_plat) as int) as is_mobile_plat_same, --手机平台是否相同
cast((operation_channel <> prev1oper_channel) as int) as is_oper_channel_same,--支付方式是否项目
cast((pay_scene <> prev1pay_scene) as int )as is_pay_scene_same,--支付场景是否相同
cast((income_card_bank_code <> prev1income_bank) as int) as is_income_bank_same,--收入银行是否相同
cast((ip_prov <> prev1ip_prov)  as int ) as is_ip_prov_same,--IP省是否相同
cast((ip_city <> prev1ip_city) as int) as is_ip_city_same,--IP市是否相同
cast((ver <> prev1version) as int)  as is_version_same,--版本号是否相同
cast((opposing_id <> prev1opposing_id) as int) is_opposing_id_same,--虚拟用户ID是否相同
cast((cert_prov <> prev1cert_prov) as int) is_cert_prov_same,--证件省是否相同
cast((cert_city <> prev1cert_city) as int) is_cert_city_same--证件市是否相同
from atec_train_data_feature_1;
--对历史交易金额的特征提取
drop table if EXISTS atec_train_data_feature_3;
create table atec_train_data_feature_3
as select 
	event_id,
    sum(amt) over (partition by user_id order by ocu_date) as amt_sum, --历史金额之和
    avg(amt) over (partition by user_id order by ocu_date) as amt_avg,--历史金额平均
    max(amt) over (partition by user_id order by ocu_date) as amt_max,--历史金额最大
    min(amt) over (partition by user_id order by ocu_date) as amt_min--历史金额最小
    from atec_train_series;
    
--过去1天历史交易金额特征
drop table if EXISTS atec_train_data_feature_4;
create table atec_train_data_feature_4
as select 
event_id,
sum(amt) as lastday_amt_sum, --lastday交易金额之和
avg(amt) as lastday_amt_avg, --lastday交易金额平均
max(amt) as lastday_amt_max, --lastday交易金额最大
min(amt) as lastday_amt_min -- lastday交易金额最小
from (select t2.amt,t1.event_id from atec_train_series t1 left outer join atec_train_series t2
on t1.user_id = t2.user_id 
where ( (t1.ocu_date >=t2.ocu_date) and (t1.ocu_date <= dateadd(t2.ocu_date,1,'day') ) ) ) tmp
group by event_id;
--过去3天历史交易金额特征
drop table if EXISTS atec_train_data_feature_5;
create table atec_train_data_feature_5
as select 
event_id,
sum(amt) as lastThreeday_amt_sum, --lastThreeday交易金额之和
avg(amt) as lastThreeday_amt_avg, --lastThreeday交易金额平均
max(amt) as lastThreeday_amt_max, --lastThreeday交易金额最大
min(amt) as lastThreeday_amt_min -- lastThreeday交易金额最小
from (select t2.amt,t1.event_id from atec_train_series t1 left outer join atec_train_series t2
on t1.user_id = t2.user_id 
where ( (t1.ocu_date >=t2.ocu_date) and (t1.ocu_date <= dateadd(t2.ocu_date,3,'day') ) ) ) tmp
group by event_id;


---for test---------------------------

