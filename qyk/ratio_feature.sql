--得到client_ip的比例值
drop table if exists atec_all_data_feature_120;
create table atec_all_data_feature_120
	as
    select 
    a.event_id,
    b.ip_ratio
    from atec_all_data_series a left join 
    (select 
    event_id,
    client_ip_count / sum_ip as ip_ratio
	from  (select event_id,
    count(1) over (partition by user_id,client_ip order by ocu_date) as client_ip_count,
    count(1) over (partition by user_id order by ocu_date) as sum_ip
    from atec_all_data_series where client_ip is not null) t )b
    on a.event_id=b.event_id;
drop table if exists atec_all_data_feature_121;
create table atec_all_data_feature_121
	as select event_id,
    case
    when ip_ratio is not null then ip_ratio
    when ip_ratio is null then 0
    end as ip_ratio
    from atec_all_data_feature_120;

--得到network的比例值
drop table if exists atec_all_data_feature_122;
create table atec_all_data_feature_122
	as
    select 
    a.event_id,
    b.network_ratio
    from atec_all_data_series a left join 
    (select 
    event_id,
    network_count / sum_network as network_ratio
	from  (select event_id,
    count(1) over (partition by user_id,network order by ocu_date) as network_count,
    count(1) over (partition by user_id order by ocu_date) as sum_network
    from atec_all_data_series where network is not null) t )b
    on a.event_id=b.event_id;
drop table if exists atec_all_data_feature_123;
create table atec_all_data_feature_123
	as select event_id,
    case
    when network_ratio is not null then network_ratio
    when network_ratio is null then 0
    end as network_ratio
    from atec_all_data_feature_122;
    
--得到device_sign的比例值
drop table if exists atec_all_data_feature_124;
create table atec_all_data_feature_124
	as
    select 
    a.event_id,
    b.device_sign_ratio
    from atec_all_data_series a left join 
    (select 
    event_id,
    device_sign_count / sum_device_sign as device_sign_ratio
	from  (select event_id,
    count(1) over (partition by user_id,device_sign order by ocu_date) as device_sign_count,
    count(1) over (partition by user_id order by ocu_date) as sum_device_sign
    from atec_all_data_series where device_sign is not null) t )b
    on a.event_id=b.event_id;
drop table if exists atec_all_data_feature_125;
create table atec_all_data_feature_125
	as select event_id,
    case
    when device_sign_ratio is not null then device_sign_ratio
    when device_sign_ratio is null then 0
    end as device_sign_ratio
    from atec_all_data_feature_124;

--得到ip_prov的比例值
drop table if exists atec_all_data_feature_126;
create table atec_all_data_feature_126
	as
    select 
    a.event_id,
    b.ip_prov_ratio
    from atec_all_data_series a left join 
    (select 
    event_id,
    ip_prov_count / sum_ip_prov as ip_prov_ratio
	from  (select event_id,
    count(1) over (partition by user_id,ip_prov order by ocu_date) as ip_prov_count,
    count(1) over (partition by user_id order by ocu_date) as sum_ip_prov
    from atec_all_data_series where ip_prov is not null) t )b
    on a.event_id=b.event_id;
drop table if exists atec_all_data_feature_127;
create table atec_all_data_feature_127
	as select event_id,
    case
    when ip_prov_ratio is not null then ip_prov_ratio
    when ip_prov_ratio is null then 0
    end as ip_prov_ratio
    from atec_all_data_feature_126;
--得到ip_city的比例值
drop table if exists atec_all_data_feature_128;
create table atec_all_data_feature_128
	as
    select 
    a.event_id,
    b.ip_city_ratio
    from atec_all_data_series a left join 
    (select 
    event_id,
    ip_city_count / sum_ip_city as ip_city_ratio
	from  (select event_id,
    count(1) over (partition by user_id,ip_city order by ocu_date) as ip_city_count,
    count(1) over (partition by user_id order by ocu_date) as sum_ip_city
    from atec_all_data_series where ip_city is not null) t )b
    on a.event_id=b.event_id;
drop table if exists atec_all_data_feature_129;
create table atec_all_data_feature_129
	as select event_id,
    case
    when ip_city_ratio is not null then ip_city_ratio
    when ip_city_ratio is null then 0
    end as ip_city_ratio
    from atec_all_data_feature_128;    
--得到card_mobile_prov的比例值
drop table if exists atec_all_data_feature_130;
create table atec_all_data_feature_130
	as
    select 
    a.event_id,
    b.card_mobile_prov_ratio
    from atec_all_data_series a left join 
    (select 
    event_id,
    card_mobile_prov_count / sum_card_mobile_prov as card_mobile_prov_ratio
	from  (select event_id,
    count(1) over (partition by user_id,card_mobile_prov order by ocu_date) as card_mobile_prov_count,
    count(1) over (partition by user_id order by ocu_date) as sum_card_mobile_prov
    from atec_all_data_series where card_mobile_prov is not null) t )b
    on a.event_id=b.event_id;
drop table if exists atec_all_data_feature_131;
create table atec_all_data_feature_131
	as select event_id,
    case
    when card_mobile_prov_ratio is not null then card_mobile_prov_ratio
    when card_mobile_prov_ratio is null then 0
    end as card_mobile_prov_ratio
    from atec_all_data_feature_130;    
--得到mobile_oper_platform的比例值
drop table if exists atec_all_data_feature_132;
create table atec_all_data_feature_132
	as
    select 
    a.event_id,
    b.mobile_oper_platform_ratio
    from atec_all_data_series a left join 
    (select 
    event_id,
    mobile_oper_platform_count / sum_mobile_oper_platform as mobile_oper_platform_ratio
	from  (select event_id,
    count(1) over (partition by user_id,mobile_oper_platform order by ocu_date) as mobile_oper_platform_count,
    count(1) over (partition by user_id order by ocu_date) as sum_mobile_oper_platform
    from atec_all_data_series where mobile_oper_platform is not null) t )b
    on a.event_id=b.event_id;
drop table if exists atec_all_data_feature_133;
create table atec_all_data_feature_133
	as select event_id,
    case
    when mobile_oper_platform_ratio is not null then mobile_oper_platform_ratio
    when mobile_oper_platform_ratio is null then 0
    end as mobile_oper_platform_ratio
    from atec_all_data_feature_132;
--得到operation_channel的比例值
drop table if exists atec_all_data_feature_134;
create table atec_all_data_feature_134
	as
    select 
    a.event_id,
    b.operation_channel_ratio
    from atec_all_data_series a left join 
    (select 
    event_id,
    operation_channel_count / sum_operation_channel as operation_channel_ratio
	from  (select event_id,
    count(1) over (partition by user_id,operation_channel order by ocu_date) as operation_channel_count,
    count(1) over (partition by user_id order by ocu_date) as sum_operation_channel
    from atec_all_data_series where operation_channel is not null) t )b
    on a.event_id=b.event_id;
drop table if exists atec_all_data_feature_135;
create table atec_all_data_feature_135
	as select event_id,
    case
    when operation_channel_ratio is not null then operation_channel_ratio
    when operation_channel_ratio is null then 0
    end as operation_channel_ratio
    from atec_all_data_feature_134;
    
--得到pay_scene的比例值
drop table if exists atec_all_data_feature_136;
create table atec_all_data_feature_136
	as
    select 
    a.event_id,
    b.pay_scene_ratio
    from atec_all_data_series a left join 
    (select 
    event_id,
    pay_scene_count / sum_pay_scene as pay_scene_ratio
	from  (select event_id,
    count(1) over (partition by user_id,pay_scene order by ocu_date) as pay_scene_count,
    count(1) over (partition by user_id order by ocu_date) as sum_pay_scene
    from atec_all_data_series where pay_scene is not null) t )b
    on a.event_id=b.event_id;
drop table if exists atec_all_data_feature_137;
create table atec_all_data_feature_137
	as select event_id,
    case
    when pay_scene_ratio is not null then pay_scene_ratio
    when pay_scene_ratio is null then 0
    end as pay_scene_ratio
    from atec_all_data_feature_136;
    
--得到card_cert_no的比例值
drop table if exists atec_all_data_feature_138;
create table atec_all_data_feature_138
	as
    select 
    a.event_id,
    b.card_cert_no_ratio
    from atec_all_data_series a left join 
    (select 
    event_id,
    card_cert_no_count / sum_card_cert_no as card_cert_no_ratio
	from  (select event_id,
    count(1) over (partition by user_id,card_cert_no order by ocu_date) as card_cert_no_count,
    count(1) over (partition by user_id order by ocu_date) as sum_card_cert_no
    from atec_all_data_series where card_cert_no is not null) t )b
    on a.event_id=b.event_id;
drop table if exists atec_all_data_feature_139;
create table atec_all_data_feature_139
	as select event_id,
    case
    when card_cert_no_ratio is not null then card_cert_no_ratio
    when card_cert_no_ratio is null then 0
    end as card_cert_no_ratio
    from atec_all_data_feature_138;
    
--得到ver的比例值
drop table if exists atec_all_data_feature_140;
create table atec_all_data_feature_140
	as
    select 
    a.event_id,
    b.ver_ratio
    from atec_all_data_series a left join 
    (select 
    event_id,
    ver_count / sum_ver as ver_ratio
	from  (select event_id,
    count(1) over (partition by user_id,ver order by ocu_date) as ver_count,
    count(1) over (partition by user_id order by ocu_date) as sum_ver
    from atec_all_data_series where ver is not null) t )b
    on a.event_id=b.event_id;
drop table if exists atec_all_data_feature_141;
create table atec_all_data_feature_141
	as select event_id,
    case
    when ver_ratio is not null then ver_ratio
    when ver_ratio is null then 0
    end as ver_ratio
    from atec_all_data_feature_140;
--得到opposing_id的比例值
drop table if exists atec_all_data_feature_142;
create table atec_all_data_feature_142
    as
    select 
    a.event_id,
    b.opposing_id_ratio
    from atec_all_data_series a left join 
    (select 
    event_id,
    opposing_id_count / sum_opposing_id as opposing_id_ratio
    from  (select event_id,
    count(1) over (partition by user_id,opposing_id order by ocu_date) as opposing_id_count,
    count(1) over (partition by user_id order by ocu_date) as sum_opposing_id
    from atec_all_data_series ) t )b
    on a.event_id=b.event_id;
--合并所有特征列
drop table if EXISTS atec_all_data_feature_145;
create table atec_all_data_feature_145
	as select 
    a.event_id,
    a.ip_ratio,--当前client_ip使用比例
    b.network_ratio,--当前network使用比例
    c.device_sign_ratio,
    d.ip_prov_ratio,
    e.ip_city_ratio,
    f.card_mobile_prov_ratio,
    g.mobile_oper_platform_ratio,
    h.operation_channel_ratio,
    i.pay_scene_ratio,
    j.card_cert_no_ratio,
    k.ver_ratio,
    l.opposing_id_ratio
    from atec_all_data_feature_121 a 
    left join atec_all_data_feature_123 b on a.event_id = b.event_id
    left join atec_all_data_feature_125 c on a.event_id = c.event_id
    left join atec_all_data_feature_127 d on a.event_id = d.event_id
    left join atec_all_data_feature_129 e on a.event_id = e.event_id
    left join atec_all_data_feature_131 f on a.event_id = f.event_id
    left join atec_all_data_feature_133 g on a.event_id = g.event_id
    left join atec_all_data_feature_135 h on a.event_id = h.event_id
    left join atec_all_data_feature_137 i on a.event_id = i.event_id
    left join atec_all_data_feature_139 j on a.event_id = j.event_id
    left join atec_all_data_feature_141 k on a.event_id = k.event_id
    left join atec_all_data_feature_142 l on a.event_id = l.event_id
    ;


--select * from atec_all_data_feature_145;
    
