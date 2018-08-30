-- user_id 在各字段下不同值的个数特征 (一小时内)
DROP TABLE IF EXISTS uid_dist_cnt_1hr
;


CREATE TABLE uid_dist_cnt_1hr AS
  SELECT event_id 
       ,COUNT(DISTINCT client_ip) OVER ( PARTITION BY user_id , month , day , ocu_hr )            AS uid_ip_dist_cnt_1hr 
       ,COUNT(DISTINCT network) OVER ( PARTITION BY user_id , month , day , ocu_hr )              AS uid_network_dist_cnt_1hr 
       ,COUNT(DISTINCT device_sign) OVER ( PARTITION BY user_id , month , day , ocu_hr )          AS uid_device_sign_dist_cnt_1hr 
       ,COUNT(DISTINCT ip_prov) OVER ( PARTITION BY user_id , month , day , ocu_hr )              AS uid_ip_prov_dist_cnt_1hr 
       ,COUNT(DISTINCT card_mobile_prov) OVER ( PARTITION BY user_id , month , day , ocu_hr )     AS uid_card_mobile_prov_dist_cnt_1hr 
       ,COUNT(DISTINCT mobile_oper_platform) OVER ( PARTITION BY user_id , month , day , ocu_hr ) AS uid_mobile_oper_platform_dist_cnt_1hr 
       ,COUNT(DISTINCT operation_channel) OVER ( PARTITION BY user_id , month , day , ocu_hr )    AS uid_operation_channel_dist_cnt_1hr 
       ,COUNT(DISTINCT pay_scene) OVER ( PARTITION BY user_id , month , day , ocu_hr )            AS uid_pay_scene_dist_cnt_1hr 
       ,COUNT(DISTINCT card_cert_no) OVER ( PARTITION BY user_id , month , day , ocu_hr )         AS uid_card_cert_no_dist_cnt_1hr 
       ,COUNT(DISTINCT ver) OVER ( PARTITION BY user_id , month , day , ocu_hr )                  AS uid_ver_dist_cnt_1hr 
       ,COUNT(DISTINCT opposing_id) OVER ( PARTITION BY user_id , month , day , ocu_hr )          AS uid_oppoid_dist_cnt_1hr 
   FROM atec_all_data_series
;


-- opposing_id 在各字段下不同值的个数特征 (一小时内)
DROP TABLE IF EXISTS oppoid_dist_cnt_1hr
;


CREATE TABLE oppoid_dist_cnt_1hr AS
  SELECT event_id 
       ,COUNT(DISTINCT user_id) OVER ( PARTITION BY opposing_id , month , day , ocu_hr )              AS oppoid_uid_dist_cnt_1hr 
       ,COUNT(DISTINCT client_ip) OVER ( PARTITION BY opposing_id , month , day , ocu_hr )            AS oppoid_ip_dist_cnt_1hr 
       ,COUNT(DISTINCT network) OVER ( PARTITION BY opposing_id , month , day , ocu_hr )              AS oppoid_network_dist_cnt_1hr 
       ,COUNT(DISTINCT device_sign) OVER ( PARTITION BY opposing_id , month , day , ocu_hr )          AS oppoid_device_sign_dist_cnt_1hr 
       ,COUNT(DISTINCT ip_prov) OVER ( PARTITION BY opposing_id , month , day , ocu_hr )              AS oppoid_ip_prov_dist_cnt_1hr 
       ,COUNT(DISTINCT card_mobile_prov) OVER ( PARTITION BY opposing_id , month , day , ocu_hr )     AS oppoid_card_mobile_prov_dist_cnt_1hr 
       ,COUNT(DISTINCT mobile_oper_platform) OVER ( PARTITION BY opposing_id , month , day , ocu_hr ) AS oppoid_mobile_oper_platform_dist_cnt_1hr 
       ,COUNT(DISTINCT operation_channel) OVER ( PARTITION BY opposing_id , month , day , ocu_hr )    AS oppoid_operation_channel_dist_cnt_1hr 
       ,COUNT(DISTINCT pay_scene) OVER ( PARTITION BY opposing_id , month , day , ocu_hr )            AS oppoid_pay_scene_dist_cnt_1hr 
       ,COUNT(DISTINCT card_cert_no) OVER ( PARTITION BY opposing_id , month , day , ocu_hr )         AS oppoid_card_cert_no_dist_cnt_1hr 
       ,COUNT(DISTINCT ver) OVER ( PARTITION BY opposing_id , month , day , ocu_hr )                  AS oppoid_ver_dist_cnt_1hr 
   FROM atec_all_data_series
;


-- client_ip 在各字段下不同值的个数特征 (一小时内)
DROP TABLE IF EXISTS ip_dist_cnt_1hr
;


CREATE TABLE ip_dist_cnt_1hr AS
  SELECT event_id 
       ,COUNT(DISTINCT user_id) OVER ( PARTITION BY client_ip , month , day , ocu_hr )              AS ip_uid_dist_cnt_1hr 
       ,COUNT(DISTINCT network) OVER ( PARTITION BY client_ip , month , day , ocu_hr )              AS ip_network_dist_cnt_1hr 
       ,COUNT(DISTINCT device_sign) OVER ( PARTITION BY client_ip , month , day , ocu_hr )          AS ip_device_dist_cnt_1hr 
       ,COUNT(DISTINCT cert_prov) OVER ( PARTITION BY client_ip , month , day , ocu_hr )            AS ip_cert_prov_dist_cnt_1hr 
       ,COUNT(DISTINCT card_mobile_prov) OVER ( PARTITION BY client_ip , month , day , ocu_hr )     AS ip_mobile_prov_dist_cnt_1hr 
       ,COUNT(DISTINCT mobile_oper_platform) OVER ( PARTITION BY client_ip , month , day , ocu_hr ) AS ip_plat_dist_cnt_1hr 
       ,COUNT(DISTINCT operation_channel) OVER ( PARTITION BY client_ip , month , day , ocu_hr )    AS ip_channel_dist_cnt_1hr 
       ,COUNT(DISTINCT pay_scene) OVER ( PARTITION BY client_ip , month , day , ocu_hr )            AS ip_pay_scene_dist_cnt_1hr 
       ,COUNT(DISTINCT card_cert_no) OVER ( PARTITION BY client_ip , month , day , ocu_hr )         AS ip_cert_no_dist_cnt_1hr 
       ,COUNT(DISTINCT opposing_id) OVER ( PARTITION BY client_ip , month , day , ocu_hr )          AS ip_opposing_id_dist_cnt_1hr 
       ,COUNT(DISTINCT ver) OVER ( PARTITION BY client_ip , month , day , ocu_hr )                  AS ip_ver_dist_cnt_1hr 
   FROM atec_all_data_series
;


