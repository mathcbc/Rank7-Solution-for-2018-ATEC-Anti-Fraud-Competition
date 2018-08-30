-- 求 client_ip 在各字段下不同值的个数特征（历史时间）

-- user_id
DROP TABLE IF EXISTS client_ip_user_id_dist_cnt
;


CREATE TABLE client_ip_user_id_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY client_ip ORDER BY ocu_date ASC ) AS client_ip_user_id_dist_cnt 
   FROM (SELECT event_id 
               ,client_ip 
               ,user_id 
               ,COUNT(*) OVER ( PARTITION BY client_ip , user_id ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE user_id IS NOT NULL 
            AND client_ip IS NOT NULL 
        )tmp
;


-- network
DROP TABLE IF EXISTS client_ip_network_dist_cnt
;


CREATE TABLE client_ip_network_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY client_ip ORDER BY ocu_date ASC ) AS client_ip_network_dist_cnt 
   FROM (SELECT event_id 
               ,client_ip 
               ,network 
               ,COUNT(*) OVER ( PARTITION BY client_ip , network ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE network IS NOT NULL 
            AND client_ip IS NOT NULL 
        )tmp
;


-- device_sign
DROP TABLE IF EXISTS client_ip_device_sign_dist_cnt
;


CREATE TABLE client_ip_device_sign_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY client_ip ORDER BY ocu_date ASC ) AS client_ip_device_sign_dist_cnt 
   FROM (SELECT event_id 
               ,client_ip 
               ,device_sign 
               ,COUNT(*) OVER ( PARTITION BY client_ip , device_sign ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE device_sign IS NOT NULL 
            AND client_ip IS NOT NULL 
        )tmp
;


-- cert_prov
DROP TABLE IF EXISTS client_ip_cert_prov_dist_cnt
;


CREATE TABLE client_ip_cert_prov_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY client_ip ORDER BY ocu_date ASC ) AS client_ip_cert_prov_dist_cnt 
   FROM (SELECT event_id 
               ,client_ip 
               ,cert_prov 
               ,COUNT(*) OVER ( PARTITION BY client_ip , cert_prov ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE cert_prov IS NOT NULL 
            AND client_ip IS NOT NULL 
        )tmp
;


-- cert_city
DROP TABLE IF EXISTS client_ip_cert_city_dist_cnt
;


CREATE TABLE client_ip_cert_city_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY client_ip ORDER BY ocu_date ASC ) AS client_ip_cert_city_dist_cnt 
   FROM (SELECT event_id 
               ,client_ip 
               ,cert_city 
               ,COUNT(*) OVER ( PARTITION BY client_ip , cert_city ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE cert_city IS NOT NULL 
            AND client_ip IS NOT NULL 
        )tmp
;


-- card_mobile_prov
DROP TABLE IF EXISTS client_ip_card_mobile_prov_dist_cnt
;


CREATE TABLE client_ip_card_mobile_prov_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY client_ip ORDER BY ocu_date ASC ) AS client_ip_card_mobile_prov_dist_cnt 
   FROM (SELECT event_id 
               ,client_ip 
               ,card_mobile_prov 
               ,COUNT(*) OVER ( PARTITION BY client_ip , card_mobile_prov ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE card_mobile_prov IS NOT NULL 
            AND client_ip IS NOT NULL 
        )tmp
;


-- card_mobile_city
DROP TABLE IF EXISTS client_ip_card_mobile_city_dist_cnt
;


CREATE TABLE client_ip_card_mobile_city_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY client_ip ORDER BY ocu_date ASC ) AS client_ip_card_mobile_city_dist_cnt 
   FROM (SELECT event_id 
               ,client_ip 
               ,card_mobile_city 
               ,COUNT(*) OVER ( PARTITION BY client_ip , card_mobile_city ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE card_mobile_city IS NOT NULL 
            AND client_ip IS NOT NULL 
        )tmp
;


-- card_cert_prov
DROP TABLE IF EXISTS client_ip_card_cert_prov_dist_cnt
;


CREATE TABLE client_ip_card_cert_prov_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY client_ip ORDER BY ocu_date ASC ) AS client_ip_card_cert_prov_dist_cnt 
   FROM (SELECT event_id 
               ,client_ip 
               ,card_cert_prov 
               ,COUNT(*) OVER ( PARTITION BY client_ip , card_cert_prov ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE card_cert_prov IS NOT NULL 
            AND client_ip IS NOT NULL 
        )tmp
;


-- card_cert_city
DROP TABLE IF EXISTS client_ip_card_cert_city_dist_cnt
;


CREATE TABLE client_ip_card_cert_city_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY client_ip ORDER BY ocu_date ASC ) AS client_ip_card_cert_city_dist_cnt 
   FROM (SELECT event_id 
               ,client_ip 
               ,card_cert_city 
               ,COUNT(*) OVER ( PARTITION BY client_ip , card_cert_city ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE card_cert_city IS NOT NULL 
            AND client_ip IS NOT NULL 
        )tmp
;


-- mobile_oper_platform
DROP TABLE IF EXISTS client_ip_mobile_oper_platform_dist_cnt
;


CREATE TABLE client_ip_mobile_oper_platform_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY client_ip ORDER BY ocu_date ASC ) AS client_ip_mobile_oper_platform_dist_cnt 
   FROM (SELECT event_id 
               ,client_ip 
               ,mobile_oper_platform 
               ,COUNT(*) OVER ( PARTITION BY client_ip , mobile_oper_platform ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE mobile_oper_platform IS NOT NULL 
            AND client_ip IS NOT NULL 
        )tmp
;


-- operation_channel
DROP TABLE IF EXISTS client_ip_operation_channel_dist_cnt
;


CREATE TABLE client_ip_operation_channel_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY client_ip ORDER BY ocu_date ASC ) AS client_ip_operation_channel_dist_cnt 
   FROM (SELECT event_id 
               ,client_ip 
               ,operation_channel 
               ,COUNT(*) OVER ( PARTITION BY client_ip , operation_channel ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE operation_channel IS NOT NULL 
            AND client_ip IS NOT NULL 
        )tmp
;


-- pay_scene
DROP TABLE IF EXISTS client_ip_pay_scene_dist_cnt
;


CREATE TABLE client_ip_pay_scene_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY client_ip ORDER BY ocu_date ASC ) AS client_ip_pay_scene_dist_cnt 
   FROM (SELECT event_id 
               ,client_ip 
               ,pay_scene 
               ,COUNT(*) OVER ( PARTITION BY client_ip , pay_scene ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE pay_scene IS NOT NULL 
            AND client_ip IS NOT NULL 
        )tmp
;


-- card_cert_no
DROP TABLE IF EXISTS client_ip_card_cert_no_dist_cnt
;


CREATE TABLE client_ip_card_cert_no_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY client_ip ORDER BY ocu_date ASC ) AS client_ip_card_cert_no_dist_cnt 
   FROM (SELECT event_id 
               ,client_ip 
               ,card_cert_no 
               ,COUNT(*) OVER ( PARTITION BY client_ip , card_cert_no ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE card_cert_no IS NOT NULL 
            AND client_ip IS NOT NULL 
        )tmp
;


-- opposing_id
DROP TABLE IF EXISTS client_ip_opposing_id_dist_cnt
;


CREATE TABLE client_ip_opposing_id_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY client_ip ORDER BY ocu_date ASC ) AS client_ip_opposing_id_dist_cnt 
   FROM (SELECT event_id 
               ,client_ip 
               ,opposing_id 
               ,COUNT(*) OVER ( PARTITION BY client_ip , opposing_id ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE opposing_id IS NOT NULL 
            AND client_ip IS NOT NULL 
        )tmp
;


-- ver
DROP TABLE IF EXISTS client_ip_ver_dist_cnt
;


CREATE TABLE client_ip_ver_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY client_ip ORDER BY ocu_date ASC ) AS client_ip_ver_dist_cnt 
   FROM (SELECT event_id 
               ,client_ip 
               ,ver 
               ,COUNT(*) OVER ( PARTITION BY client_ip , ver ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE ver IS NOT NULL 
            AND client_ip IS NOT NULL 
        )tmp
;


DROP TABLE IF EXISTS client_ip_dist_cnt_feature_cbc
;


CREATE TABLE client_ip_dist_cnt_feature_cbc AS
  SELECT t1.event_id 
       ,client_ip_user_id_dist_cnt 
       ,client_ip_network_dist_cnt 
       ,client_ip_device_sign_dist_cnt 
       ,client_ip_cert_prov_dist_cnt 
       ,client_ip_cert_city_dist_cnt 
       ,client_ip_card_mobile_prov_dist_cnt 
       ,client_ip_card_mobile_city_dist_cnt 
       ,client_ip_card_cert_prov_dist_cnt 
       ,client_ip_card_cert_city_dist_cnt 
       ,client_ip_mobile_oper_platform_dist_cnt 
       ,client_ip_operation_channel_dist_cnt 
       ,client_ip_pay_scene_dist_cnt 
       ,client_ip_card_cert_no_dist_cnt 
       ,client_ip_opposing_id_dist_cnt 
       ,client_ip_ver_dist_cnt 
   FROM (SELECT event_id 
           FROM atec_all_data_series 
        )t1 
 LEFT OUTER JOIN client_ip_user_id_dist_cnt 
     ON client_ip_user_id_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN client_ip_network_dist_cnt 
     ON client_ip_network_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN client_ip_device_sign_dist_cnt 
     ON client_ip_device_sign_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN client_ip_cert_prov_dist_cnt 
     ON client_ip_cert_prov_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN client_ip_cert_city_dist_cnt 
     ON client_ip_cert_city_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN client_ip_card_mobile_prov_dist_cnt 
     ON client_ip_card_mobile_prov_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN client_ip_card_mobile_city_dist_cnt 
     ON client_ip_card_mobile_city_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN client_ip_card_cert_prov_dist_cnt 
     ON client_ip_card_cert_prov_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN client_ip_card_cert_city_dist_cnt 
     ON client_ip_card_cert_city_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN client_ip_mobile_oper_platform_dist_cnt 
     ON client_ip_mobile_oper_platform_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN client_ip_operation_channel_dist_cnt 
     ON client_ip_operation_channel_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN client_ip_pay_scene_dist_cnt 
     ON client_ip_pay_scene_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN client_ip_card_cert_no_dist_cnt 
     ON client_ip_card_cert_no_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN client_ip_opposing_id_dist_cnt 
     ON client_ip_opposing_id_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN client_ip_ver_dist_cnt 
     ON client_ip_ver_dist_cnt.event_id = t1.event_id
;


