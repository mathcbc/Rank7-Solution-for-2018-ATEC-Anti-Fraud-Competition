-- 求 device_sign 在各字段下不同值的个数特征（历史时间）

-- user_id
DROP TABLE IF EXISTS device_sign_user_id_dist_cnt
;


CREATE TABLE device_sign_user_id_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY device_sign ORDER BY ocu_date ASC ) AS device_sign_user_id_dist_cnt 
   FROM (SELECT event_id 
               ,device_sign 
               ,user_id 
               ,COUNT(*) OVER ( PARTITION BY device_sign , user_id ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE user_id IS NOT NULL 
            AND device_sign IS NOT NULL 
        )tmp
;


-- client_ip
DROP TABLE IF EXISTS device_sign_client_ip_dist_cnt
;


CREATE TABLE device_sign_client_ip_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY device_sign ORDER BY ocu_date ASC ) AS device_sign_client_ip_dist_cnt 
   FROM (SELECT event_id 
               ,device_sign 
               ,client_ip 
               ,COUNT(*) OVER ( PARTITION BY device_sign , client_ip ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE client_ip IS NOT NULL 
            AND device_sign IS NOT NULL 
        )tmp
;


-- network
DROP TABLE IF EXISTS device_sign_network_dist_cnt
;


CREATE TABLE device_sign_network_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY device_sign ORDER BY ocu_date ASC ) AS device_sign_network_dist_cnt 
   FROM (SELECT event_id 
               ,device_sign 
               ,network 
               ,COUNT(*) OVER ( PARTITION BY device_sign , network ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE network IS NOT NULL 
            AND device_sign IS NOT NULL 
        )tmp
;


-- ip_prov
DROP TABLE IF EXISTS device_sign_ip_prov_dist_cnt
;


CREATE TABLE device_sign_ip_prov_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY device_sign ORDER BY ocu_date ASC ) AS device_sign_ip_prov_dist_cnt 
   FROM (SELECT event_id 
               ,device_sign 
               ,ip_prov 
               ,COUNT(*) OVER ( PARTITION BY device_sign , ip_prov ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE ip_prov IS NOT NULL 
            AND device_sign IS NOT NULL 
        )tmp
;


-- ip_city
DROP TABLE IF EXISTS device_sign_ip_city_dist_cnt
;


CREATE TABLE device_sign_ip_city_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY device_sign ORDER BY ocu_date ASC ) AS device_sign_ip_city_dist_cnt 
   FROM (SELECT event_id 
               ,device_sign 
               ,ip_city 
               ,COUNT(*) OVER ( PARTITION BY device_sign , ip_city ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE ip_city IS NOT NULL 
            AND device_sign IS NOT NULL 
        )tmp
;


-- cert_prov
DROP TABLE IF EXISTS device_sign_cert_prov_dist_cnt
;


CREATE TABLE device_sign_cert_prov_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY device_sign ORDER BY ocu_date ASC ) AS device_sign_cert_prov_dist_cnt 
   FROM (SELECT event_id 
               ,device_sign 
               ,cert_prov 
               ,COUNT(*) OVER ( PARTITION BY device_sign , cert_prov ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE cert_prov IS NOT NULL 
            AND device_sign IS NOT NULL 
        )tmp
;


-- cert_city
DROP TABLE IF EXISTS device_sign_cert_city_dist_cnt
;


CREATE TABLE device_sign_cert_city_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY device_sign ORDER BY ocu_date ASC ) AS device_sign_cert_city_dist_cnt 
   FROM (SELECT event_id 
               ,device_sign 
               ,cert_city 
               ,COUNT(*) OVER ( PARTITION BY device_sign , cert_city ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE cert_city IS NOT NULL 
            AND device_sign IS NOT NULL 
        )tmp
;


-- card_mobile_prov
DROP TABLE IF EXISTS device_sign_card_mobile_prov_dist_cnt
;


CREATE TABLE device_sign_card_mobile_prov_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY device_sign ORDER BY ocu_date ASC ) AS device_sign_card_mobile_prov_dist_cnt 
   FROM (SELECT event_id 
               ,device_sign 
               ,card_mobile_prov 
               ,COUNT(*) OVER ( PARTITION BY device_sign , card_mobile_prov ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE card_mobile_prov IS NOT NULL 
            AND device_sign IS NOT NULL 
        )tmp
;


-- card_mobile_city
DROP TABLE IF EXISTS device_sign_card_mobile_city_dist_cnt
;


CREATE TABLE device_sign_card_mobile_city_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY device_sign ORDER BY ocu_date ASC ) AS device_sign_card_mobile_city_dist_cnt 
   FROM (SELECT event_id 
               ,device_sign 
               ,card_mobile_city 
               ,COUNT(*) OVER ( PARTITION BY device_sign , card_mobile_city ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE card_mobile_city IS NOT NULL 
            AND device_sign IS NOT NULL 
        )tmp
;


-- card_cert_prov
DROP TABLE IF EXISTS device_sign_card_cert_prov_dist_cnt
;


CREATE TABLE device_sign_card_cert_prov_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY device_sign ORDER BY ocu_date ASC ) AS device_sign_card_cert_prov_dist_cnt 
   FROM (SELECT event_id 
               ,device_sign 
               ,card_cert_prov 
               ,COUNT(*) OVER ( PARTITION BY device_sign , card_cert_prov ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE card_cert_prov IS NOT NULL 
            AND device_sign IS NOT NULL 
        )tmp
;


-- card_cert_city
DROP TABLE IF EXISTS device_sign_card_cert_city_dist_cnt
;


CREATE TABLE device_sign_card_cert_city_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY device_sign ORDER BY ocu_date ASC ) AS device_sign_card_cert_city_dist_cnt 
   FROM (SELECT event_id 
               ,device_sign 
               ,card_cert_city 
               ,COUNT(*) OVER ( PARTITION BY device_sign , card_cert_city ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE card_cert_city IS NOT NULL 
            AND device_sign IS NOT NULL 
        )tmp
;


-- mobile_oper_platform
DROP TABLE IF EXISTS device_sign_mobile_oper_platform_dist_cnt
;


CREATE TABLE device_sign_mobile_oper_platform_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY device_sign ORDER BY ocu_date ASC ) AS device_sign_mobile_oper_platform_dist_cnt 
   FROM (SELECT event_id 
               ,device_sign 
               ,mobile_oper_platform 
               ,COUNT(*) OVER ( PARTITION BY device_sign , mobile_oper_platform ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE mobile_oper_platform IS NOT NULL 
            AND device_sign IS NOT NULL 
        )tmp
;


-- operation_channel
DROP TABLE IF EXISTS device_sign_operation_channel_dist_cnt
;


CREATE TABLE device_sign_operation_channel_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY device_sign ORDER BY ocu_date ASC ) AS device_sign_operation_channel_dist_cnt 
   FROM (SELECT event_id 
               ,device_sign 
               ,operation_channel 
               ,COUNT(*) OVER ( PARTITION BY device_sign , operation_channel ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE operation_channel IS NOT NULL 
            AND device_sign IS NOT NULL 
        )tmp
;


-- pay_scene
DROP TABLE IF EXISTS device_sign_pay_scene_dist_cnt
;


CREATE TABLE device_sign_pay_scene_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY device_sign ORDER BY ocu_date ASC ) AS device_sign_pay_scene_dist_cnt 
   FROM (SELECT event_id 
               ,device_sign 
               ,pay_scene 
               ,COUNT(*) OVER ( PARTITION BY device_sign , pay_scene ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE pay_scene IS NOT NULL 
            AND device_sign IS NOT NULL 
        )tmp
;


-- card_cert_no
DROP TABLE IF EXISTS device_sign_card_cert_no_dist_cnt
;


CREATE TABLE device_sign_card_cert_no_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY device_sign ORDER BY ocu_date ASC ) AS device_sign_card_cert_no_dist_cnt 
   FROM (SELECT event_id 
               ,device_sign 
               ,card_cert_no 
               ,COUNT(*) OVER ( PARTITION BY device_sign , card_cert_no ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE card_cert_no IS NOT NULL 
            AND device_sign IS NOT NULL 
        )tmp
;


-- opposing_id
DROP TABLE IF EXISTS device_sign_opposing_id_dist_cnt
;


CREATE TABLE device_sign_opposing_id_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY device_sign ORDER BY ocu_date ASC ) AS device_sign_opposing_id_dist_cnt 
   FROM (SELECT event_id 
               ,device_sign 
               ,opposing_id 
               ,COUNT(*) OVER ( PARTITION BY device_sign , opposing_id ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE opposing_id IS NOT NULL 
            AND device_sign IS NOT NULL 
        )tmp
;


-- ver
DROP TABLE IF EXISTS device_sign_ver_dist_cnt
;


CREATE TABLE device_sign_ver_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY device_sign ORDER BY ocu_date ASC ) AS device_sign_ver_dist_cnt 
   FROM (SELECT event_id 
               ,device_sign 
               ,ver 
               ,COUNT(*) OVER ( PARTITION BY device_sign , ver ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE ver IS NOT NULL 
            AND device_sign IS NOT NULL 
        )tmp
;


DROP TABLE IF EXISTS device_sign_dist_cnt_feature_cbc
;


CREATE TABLE device_sign_dist_cnt_feature_cbc AS
  SELECT t1.event_id 
       ,device_sign_user_id_dist_cnt 
       ,device_sign_client_ip_dist_cnt 
       ,device_sign_network_dist_cnt 
       ,device_sign_ip_prov_dist_cnt 
       ,device_sign_ip_city_dist_cnt 
       ,device_sign_cert_prov_dist_cnt 
       ,device_sign_cert_city_dist_cnt 
       ,device_sign_card_mobile_prov_dist_cnt 
       ,device_sign_card_mobile_city_dist_cnt 
       ,device_sign_card_cert_prov_dist_cnt 
       ,device_sign_card_cert_city_dist_cnt 
       ,device_sign_mobile_oper_platform_dist_cnt 
       ,device_sign_operation_channel_dist_cnt 
       ,device_sign_pay_scene_dist_cnt 
       ,device_sign_card_cert_no_dist_cnt 
       ,device_sign_opposing_id_dist_cnt 
       ,device_sign_ver_dist_cnt 
   FROM (SELECT event_id 
           FROM atec_all_data_series 
        )t1 
 LEFT OUTER JOIN device_sign_user_id_dist_cnt 
     ON device_sign_user_id_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN device_sign_client_ip_dist_cnt 
     ON device_sign_client_ip_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN device_sign_network_dist_cnt 
     ON device_sign_network_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN device_sign_ip_prov_dist_cnt 
     ON device_sign_ip_prov_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN device_sign_ip_city_dist_cnt 
     ON device_sign_ip_city_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN device_sign_cert_prov_dist_cnt 
     ON device_sign_cert_prov_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN device_sign_cert_city_dist_cnt 
     ON device_sign_cert_city_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN device_sign_card_mobile_prov_dist_cnt 
     ON device_sign_card_mobile_prov_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN device_sign_card_mobile_city_dist_cnt 
     ON device_sign_card_mobile_city_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN device_sign_card_cert_prov_dist_cnt 
     ON device_sign_card_cert_prov_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN device_sign_card_cert_city_dist_cnt 
     ON device_sign_card_cert_city_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN device_sign_mobile_oper_platform_dist_cnt 
     ON device_sign_mobile_oper_platform_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN device_sign_operation_channel_dist_cnt 
     ON device_sign_operation_channel_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN device_sign_pay_scene_dist_cnt 
     ON device_sign_pay_scene_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN device_sign_card_cert_no_dist_cnt 
     ON device_sign_card_cert_no_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN device_sign_opposing_id_dist_cnt 
     ON device_sign_opposing_id_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN device_sign_ver_dist_cnt 
     ON device_sign_ver_dist_cnt.event_id = t1.event_id
;


