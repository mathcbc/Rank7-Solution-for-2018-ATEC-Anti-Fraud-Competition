-- 求 user_id 在历史时间里各字段的众数

-- 当前 client_ip 是否 user_id 使用的众数
DROP TABLE IF EXISTS user_id_client_ip_is_mode_cbc
;


CREATE TABLE user_id_client_ip_is_mode_cbc AS
  SELECT event_id 
       ,CAST ( tmp_cnt = MAX(tmp_cnt) OVER ( PARTITION BY user_id ORDER BY ocu_date ASC ) AS INT ) AS user_id_client_ip_is_mode 
   FROM (SELECT event_id 
               ,user_id 
               ,COUNT(*) OVER ( PARTITION BY user_id , client_ip ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE user_id IS NOT NULL 
            AND client_ip IS NOT NULL 
        )tmp
;


-- 当前 network 是否 user_id 使用的众数
DROP TABLE IF EXISTS user_id_network_is_mode_cbc
;


CREATE TABLE user_id_network_is_mode_cbc AS
  SELECT event_id 
       ,CAST ( tmp_cnt = MAX(tmp_cnt) OVER ( PARTITION BY user_id ORDER BY ocu_date ASC ) AS INT ) AS user_id_network_is_mode 
   FROM (SELECT event_id 
               ,user_id 
               ,COUNT(*) OVER ( PARTITION BY user_id , network ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE user_id IS NOT NULL 
            AND network IS NOT NULL 
        )tmp
;


-- 当前 device_sign 是否 user_id 使用的众数
DROP TABLE IF EXISTS user_id_device_sign_is_mode_cbc
;


CREATE TABLE user_id_device_sign_is_mode_cbc AS
  SELECT event_id 
       ,CAST ( tmp_cnt = MAX(tmp_cnt) OVER ( PARTITION BY user_id ORDER BY ocu_date ASC ) AS INT ) AS user_id_device_sign_is_mode 
   FROM (SELECT event_id 
               ,user_id 
               ,COUNT(*) OVER ( PARTITION BY user_id , device_sign ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE user_id IS NOT NULL 
            AND device_sign IS NOT NULL 
        )tmp
;


-- 当前 ip_prov 是否 user_id 使用的众数
DROP TABLE IF EXISTS user_id_ip_prov_is_mode_cbc
;


CREATE TABLE user_id_ip_prov_is_mode_cbc AS
  SELECT event_id 
       ,CAST ( tmp_cnt = MAX(tmp_cnt) OVER ( PARTITION BY user_id ORDER BY ocu_date ASC ) AS INT ) AS user_id_ip_prov_is_mode 
   FROM (SELECT event_id 
               ,user_id 
               ,COUNT(*) OVER ( PARTITION BY user_id , ip_prov ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE user_id IS NOT NULL 
            AND ip_prov IS NOT NULL 
        )tmp
;


-- 当前 ip_city 是否 user_id 使用的众数
DROP TABLE IF EXISTS user_id_ip_city_is_mode_cbc
;


CREATE TABLE user_id_ip_city_is_mode_cbc AS
  SELECT event_id 
       ,CAST ( tmp_cnt = MAX(tmp_cnt) OVER ( PARTITION BY user_id ORDER BY ocu_date ASC ) AS INT ) AS user_id_ip_city_is_mode 
   FROM (SELECT event_id 
               ,user_id 
               ,COUNT(*) OVER ( PARTITION BY user_id , ip_city ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE user_id IS NOT NULL 
            AND ip_city IS NOT NULL 
        )tmp
;


-- 当前 cert_prov 是否 user_id 使用的众数
DROP TABLE IF EXISTS user_id_cert_prov_is_mode_cbc
;


CREATE TABLE user_id_cert_prov_is_mode_cbc AS
  SELECT event_id 
       ,CAST ( tmp_cnt = MAX(tmp_cnt) OVER ( PARTITION BY user_id ORDER BY ocu_date ASC ) AS INT ) AS user_id_cert_prov_is_mode 
   FROM (SELECT event_id 
               ,user_id 
               ,COUNT(*) OVER ( PARTITION BY user_id , cert_prov ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE user_id IS NOT NULL 
            AND cert_prov IS NOT NULL 
        )tmp
;


-- 当前 cert_city 是否 user_id 使用的众数
DROP TABLE IF EXISTS user_id_cert_city_is_mode_cbc
;


CREATE TABLE user_id_cert_city_is_mode_cbc AS
  SELECT event_id 
       ,CAST ( tmp_cnt = MAX(tmp_cnt) OVER ( PARTITION BY user_id ORDER BY ocu_date ASC ) AS INT ) AS user_id_cert_city_is_mode 
   FROM (SELECT event_id 
               ,user_id 
               ,COUNT(*) OVER ( PARTITION BY user_id , cert_city ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE user_id IS NOT NULL 
            AND cert_city IS NOT NULL 
        )tmp
;


-- 当前 card_mobile_prov 是否 user_id 使用的众数
DROP TABLE IF EXISTS user_id_card_mobile_prov_is_mode_cbc
;


CREATE TABLE user_id_card_mobile_prov_is_mode_cbc AS
  SELECT event_id 
       ,CAST ( tmp_cnt = MAX(tmp_cnt) OVER ( PARTITION BY user_id ORDER BY ocu_date ASC ) AS INT ) AS user_id_card_mobile_prov_is_mode 
   FROM (SELECT event_id 
               ,user_id 
               ,COUNT(*) OVER ( PARTITION BY user_id , card_mobile_prov ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE user_id IS NOT NULL 
            AND card_mobile_prov IS NOT NULL 
        )tmp
;


-- 当前 card_mobile_city 是否 user_id 使用的众数
DROP TABLE IF EXISTS user_id_card_mobile_city_is_mode_cbc
;


CREATE TABLE user_id_card_mobile_city_is_mode_cbc AS
  SELECT event_id 
       ,CAST ( tmp_cnt = MAX(tmp_cnt) OVER ( PARTITION BY user_id ORDER BY ocu_date ASC ) AS INT ) AS user_id_card_mobile_city_is_mode 
   FROM (SELECT event_id 
               ,user_id 
               ,COUNT(*) OVER ( PARTITION BY user_id , card_mobile_city ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE user_id IS NOT NULL 
            AND card_mobile_city IS NOT NULL 
        )tmp
;


-- 当前 card_cert_prov 是否 user_id 使用的众数
DROP TABLE IF EXISTS user_id_card_cert_prov_is_mode_cbc
;


CREATE TABLE user_id_card_cert_prov_is_mode_cbc AS
  SELECT event_id 
       ,CAST ( tmp_cnt = MAX(tmp_cnt) OVER ( PARTITION BY user_id ORDER BY ocu_date ASC ) AS INT ) AS user_id_card_cert_prov_is_mode 
   FROM (SELECT event_id 
               ,user_id 
               ,COUNT(*) OVER ( PARTITION BY user_id , card_cert_prov ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE user_id IS NOT NULL 
            AND card_cert_prov IS NOT NULL 
        )tmp
;


-- 当前 card_cert_city 是否 user_id 使用的众数
DROP TABLE IF EXISTS user_id_card_cert_city_is_mode_cbc
;


CREATE TABLE user_id_card_cert_city_is_mode_cbc AS
  SELECT event_id 
       ,CAST ( tmp_cnt = MAX(tmp_cnt) OVER ( PARTITION BY user_id ORDER BY ocu_date ASC ) AS INT ) AS user_id_card_cert_city_is_mode 
   FROM (SELECT event_id 
               ,user_id 
               ,COUNT(*) OVER ( PARTITION BY user_id , card_cert_city ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE user_id IS NOT NULL 
            AND card_cert_city IS NOT NULL 
        )tmp
;


-- 当前 mobile_oper_platform 是否 user_id 使用的众数
DROP TABLE IF EXISTS user_id_mobile_oper_platform_is_mode_cbc
;


CREATE TABLE user_id_mobile_oper_platform_is_mode_cbc AS
  SELECT event_id 
       ,CAST ( tmp_cnt = MAX(tmp_cnt) OVER ( PARTITION BY user_id ORDER BY ocu_date ASC ) AS INT ) AS user_id_mobile_oper_platform_is_mode 
   FROM (SELECT event_id 
               ,user_id 
               ,COUNT(*) OVER ( PARTITION BY user_id , mobile_oper_platform ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE user_id IS NOT NULL 
            AND mobile_oper_platform IS NOT NULL 
        )tmp
;


-- 当前 operation_channel 是否 user_id 使用的众数
DROP TABLE IF EXISTS user_id_operation_channel_is_mode_cbc
;


CREATE TABLE user_id_operation_channel_is_mode_cbc AS
  SELECT event_id 
       ,CAST ( tmp_cnt = MAX(tmp_cnt) OVER ( PARTITION BY user_id ORDER BY ocu_date ASC ) AS INT ) AS user_id_operation_channel_is_mode 
   FROM (SELECT event_id 
               ,user_id 
               ,COUNT(*) OVER ( PARTITION BY user_id , operation_channel ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE user_id IS NOT NULL 
            AND operation_channel IS NOT NULL 
        )tmp
;


-- 当前 pay_scene 是否 user_id 使用的众数
DROP TABLE IF EXISTS user_id_pay_scene_is_mode_cbc
;


CREATE TABLE user_id_pay_scene_is_mode_cbc AS
  SELECT event_id 
       ,CAST ( tmp_cnt = MAX(tmp_cnt) OVER ( PARTITION BY user_id ORDER BY ocu_date ASC ) AS INT ) AS user_id_pay_scene_is_mode 
   FROM (SELECT event_id 
               ,user_id 
               ,COUNT(*) OVER ( PARTITION BY user_id , pay_scene ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE user_id IS NOT NULL 
            AND pay_scene IS NOT NULL 
        )tmp
;


-- 当前 card_cert_no 是否 user_id 使用的众数
DROP TABLE IF EXISTS user_id_card_cert_no_is_mode_cbc
;


CREATE TABLE user_id_card_cert_no_is_mode_cbc AS
  SELECT event_id 
       ,CAST ( tmp_cnt = MAX(tmp_cnt) OVER ( PARTITION BY user_id ORDER BY ocu_date ASC ) AS INT ) AS user_id_card_cert_no_is_mode 
   FROM (SELECT event_id 
               ,user_id 
               ,COUNT(*) OVER ( PARTITION BY user_id , card_cert_no ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE user_id IS NOT NULL 
            AND card_cert_no IS NOT NULL 
        )tmp
;


-- 当前 opposing_id 是否 user_id 使用的众数
DROP TABLE IF EXISTS user_id_opposing_id_is_mode_cbc
;


CREATE TABLE user_id_opposing_id_is_mode_cbc AS
  SELECT event_id 
       ,CAST ( tmp_cnt = MAX(tmp_cnt) OVER ( PARTITION BY user_id ORDER BY ocu_date ASC ) AS INT ) AS user_id_opposing_id_is_mode 
   FROM (SELECT event_id 
               ,user_id 
               ,COUNT(*) OVER ( PARTITION BY user_id , opposing_id ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE user_id IS NOT NULL 
            AND opposing_id IS NOT NULL 
        )tmp
;


-- 当前 ver 是否 user_id 使用的众数
DROP TABLE IF EXISTS user_id_ver_is_mode_cbc
;


CREATE TABLE user_id_ver_is_mode_cbc AS
  SELECT event_id 
       ,CAST ( tmp_cnt = MAX(tmp_cnt) OVER ( PARTITION BY user_id ORDER BY ocu_date ASC ) AS INT ) AS user_id_ver_is_mode 
   FROM (SELECT event_id 
               ,user_id 
               ,COUNT(*) OVER ( PARTITION BY user_id , ver ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE user_id IS NOT NULL 
            AND ver IS NOT NULL 
        )tmp
;


DROP TABLE IF EXISTS user_id_is_mode_feature_cbc
;


CREATE TABLE user_id_is_mode_feature_cbc AS
  SELECT t1.event_id 
       ,user_id_client_ip_is_mode            AS ip_is_most 
       ,user_id_network_is_mode              AS network_is_most 
       ,user_id_device_sign_is_mode          AS device_sign_is_most 
       ,user_id_ip_prov_is_mode              AS ip_prov_is_most 
       ,user_id_ip_city_is_mode              AS ip_city_is_most 
       ,user_id_cert_prov_is_mode 
       ,user_id_cert_city_is_mode 
       ,user_id_card_mobile_prov_is_mode 
       ,user_id_card_mobile_city_is_mode 
       ,user_id_card_cert_prov_is_mode 
       ,user_id_card_cert_city_is_mode 
       ,user_id_mobile_oper_platform_is_mode AS mobile_oper_platform_is_most 
       ,user_id_operation_channel_is_mode    AS operation_channel_is_most 
       ,user_id_pay_scene_is_mode            AS pay_scene_is_most 
       ,user_id_card_cert_no_is_mode 
       ,user_id_opposing_id_is_mode 
       ,user_id_ver_is_mode 
   FROM (SELECT event_id 
           FROM atec_all_data_series 
        )t1 
 LEFT OUTER JOIN user_id_client_ip_is_mode_cbc 
     ON user_id_client_ip_is_mode_cbc.event_id = t1.event_id 
 LEFT OUTER JOIN user_id_network_is_mode_cbc 
     ON user_id_network_is_mode_cbc.event_id = t1.event_id 
 LEFT OUTER JOIN user_id_device_sign_is_mode_cbc 
     ON user_id_device_sign_is_mode_cbc.event_id = t1.event_id 
 LEFT OUTER JOIN user_id_ip_prov_is_mode_cbc 
     ON user_id_ip_prov_is_mode_cbc.event_id = t1.event_id 
 LEFT OUTER JOIN user_id_ip_city_is_mode_cbc 
     ON user_id_ip_city_is_mode_cbc.event_id = t1.event_id 
 LEFT OUTER JOIN user_id_cert_prov_is_mode_cbc 
     ON user_id_cert_prov_is_mode_cbc.event_id = t1.event_id 
 LEFT OUTER JOIN user_id_cert_city_is_mode_cbc 
     ON user_id_cert_city_is_mode_cbc.event_id = t1.event_id 
 LEFT OUTER JOIN user_id_card_mobile_prov_is_mode_cbc 
     ON user_id_card_mobile_prov_is_mode_cbc.event_id = t1.event_id 
 LEFT OUTER JOIN user_id_card_mobile_city_is_mode_cbc 
     ON user_id_card_mobile_city_is_mode_cbc.event_id = t1.event_id 
 LEFT OUTER JOIN user_id_card_cert_prov_is_mode_cbc 
     ON user_id_card_cert_prov_is_mode_cbc.event_id = t1.event_id 
 LEFT OUTER JOIN user_id_card_cert_city_is_mode_cbc 
     ON user_id_card_cert_city_is_mode_cbc.event_id = t1.event_id 
 LEFT OUTER JOIN user_id_mobile_oper_platform_is_mode_cbc 
     ON user_id_mobile_oper_platform_is_mode_cbc.event_id = t1.event_id 
 LEFT OUTER JOIN user_id_operation_channel_is_mode_cbc 
     ON user_id_operation_channel_is_mode_cbc.event_id = t1.event_id 
 LEFT OUTER JOIN user_id_pay_scene_is_mode_cbc 
     ON user_id_pay_scene_is_mode_cbc.event_id = t1.event_id 
 LEFT OUTER JOIN user_id_card_cert_no_is_mode_cbc 
     ON user_id_card_cert_no_is_mode_cbc.event_id = t1.event_id 
 LEFT OUTER JOIN user_id_opposing_id_is_mode_cbc 
     ON user_id_opposing_id_is_mode_cbc.event_id = t1.event_id 
 LEFT OUTER JOIN user_id_ver_is_mode_cbc 
     ON user_id_ver_is_mode_cbc.event_id = t1.event_id
;


