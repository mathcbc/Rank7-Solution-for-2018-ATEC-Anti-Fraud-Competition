-- 求 opposing_id 在各字段下不同值的个数特征（历史时间）

-- user_id
DROP TABLE IF EXISTS opposing_id_user_id_dist_cnt
;


CREATE TABLE opposing_id_user_id_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC ) AS opposing_id_user_id_dist_cnt 
   FROM (SELECT event_id 
               ,opposing_id 
               ,user_id 
               ,COUNT(*) OVER ( PARTITION BY opposing_id , user_id ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE user_id IS NOT NULL 
            AND opposing_id IS NOT NULL 
        )tmp
;


-- client_ip
DROP TABLE IF EXISTS opposing_id_client_ip_dist_cnt
;


CREATE TABLE opposing_id_client_ip_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC ) AS opposing_id_client_ip_dist_cnt 
   FROM (SELECT event_id 
               ,opposing_id 
               ,client_ip 
               ,COUNT(*) OVER ( PARTITION BY opposing_id , client_ip ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE client_ip IS NOT NULL 
            AND opposing_id IS NOT NULL 
        )tmp
;


-- network
DROP TABLE IF EXISTS opposing_id_network_dist_cnt
;


CREATE TABLE opposing_id_network_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC ) AS opposing_id_network_dist_cnt 
   FROM (SELECT event_id 
               ,opposing_id 
               ,network 
               ,COUNT(*) OVER ( PARTITION BY opposing_id , network ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE network IS NOT NULL 
            AND opposing_id IS NOT NULL 
        )tmp
;


-- device_sign
DROP TABLE IF EXISTS opposing_id_device_sign_dist_cnt
;


CREATE TABLE opposing_id_device_sign_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC ) AS opposing_id_device_sign_dist_cnt 
   FROM (SELECT event_id 
               ,opposing_id 
               ,device_sign 
               ,COUNT(*) OVER ( PARTITION BY opposing_id , device_sign ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE device_sign IS NOT NULL 
            AND opposing_id IS NOT NULL 
        )tmp
;


-- ip_prov
DROP TABLE IF EXISTS opposing_id_ip_prov_dist_cnt
;


CREATE TABLE opposing_id_ip_prov_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC ) AS opposing_id_ip_prov_dist_cnt 
   FROM (SELECT event_id 
               ,opposing_id 
               ,ip_prov 
               ,COUNT(*) OVER ( PARTITION BY opposing_id , ip_prov ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE ip_prov IS NOT NULL 
            AND opposing_id IS NOT NULL 
        )tmp
;


-- ip_city
DROP TABLE IF EXISTS opposing_id_ip_city_dist_cnt
;


CREATE TABLE opposing_id_ip_city_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC ) AS opposing_id_ip_city_dist_cnt 
   FROM (SELECT event_id 
               ,opposing_id 
               ,ip_city 
               ,COUNT(*) OVER ( PARTITION BY opposing_id , ip_city ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE ip_city IS NOT NULL 
            AND opposing_id IS NOT NULL 
        )tmp
;


-- cert_prov
DROP TABLE IF EXISTS opposing_id_cert_prov_dist_cnt
;


CREATE TABLE opposing_id_cert_prov_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC ) AS opposing_id_cert_prov_dist_cnt 
   FROM (SELECT event_id 
               ,opposing_id 
               ,cert_prov 
               ,COUNT(*) OVER ( PARTITION BY opposing_id , cert_prov ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE cert_prov IS NOT NULL 
            AND opposing_id IS NOT NULL 
        )tmp
;


-- cert_city
DROP TABLE IF EXISTS opposing_id_cert_city_dist_cnt
;


CREATE TABLE opposing_id_cert_city_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC ) AS opposing_id_cert_city_dist_cnt 
   FROM (SELECT event_id 
               ,opposing_id 
               ,cert_city 
               ,COUNT(*) OVER ( PARTITION BY opposing_id , cert_city ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE cert_city IS NOT NULL 
            AND opposing_id IS NOT NULL 
        )tmp
;


-- card_mobile_prov
DROP TABLE IF EXISTS opposing_id_card_mobile_prov_dist_cnt
;


CREATE TABLE opposing_id_card_mobile_prov_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC ) AS opposing_id_card_mobile_prov_dist_cnt 
   FROM (SELECT event_id 
               ,opposing_id 
               ,card_mobile_prov 
               ,COUNT(*) OVER ( PARTITION BY opposing_id , card_mobile_prov ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE card_mobile_prov IS NOT NULL 
            AND opposing_id IS NOT NULL 
        )tmp
;


-- card_mobile_city
DROP TABLE IF EXISTS opposing_id_card_mobile_city_dist_cnt
;


CREATE TABLE opposing_id_card_mobile_city_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC ) AS opposing_id_card_mobile_city_dist_cnt 
   FROM (SELECT event_id 
               ,opposing_id 
               ,card_mobile_city 
               ,COUNT(*) OVER ( PARTITION BY opposing_id , card_mobile_city ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE card_mobile_city IS NOT NULL 
            AND opposing_id IS NOT NULL 
        )tmp
;


-- card_cert_prov
DROP TABLE IF EXISTS opposing_id_card_cert_prov_dist_cnt
;


CREATE TABLE opposing_id_card_cert_prov_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC ) AS opposing_id_card_cert_prov_dist_cnt 
   FROM (SELECT event_id 
               ,opposing_id 
               ,card_cert_prov 
               ,COUNT(*) OVER ( PARTITION BY opposing_id , card_cert_prov ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE card_cert_prov IS NOT NULL 
            AND opposing_id IS NOT NULL 
        )tmp
;


-- card_cert_city
DROP TABLE IF EXISTS opposing_id_card_cert_city_dist_cnt
;


CREATE TABLE opposing_id_card_cert_city_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC ) AS opposing_id_card_cert_city_dist_cnt 
   FROM (SELECT event_id 
               ,opposing_id 
               ,card_cert_city 
               ,COUNT(*) OVER ( PARTITION BY opposing_id , card_cert_city ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE card_cert_city IS NOT NULL 
            AND opposing_id IS NOT NULL 
        )tmp
;


-- mobile_oper_platform
DROP TABLE IF EXISTS opposing_id_mobile_oper_platform_dist_cnt
;


CREATE TABLE opposing_id_mobile_oper_platform_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC ) AS opposing_id_mobile_oper_platform_dist_cnt 
   FROM (SELECT event_id 
               ,opposing_id 
               ,mobile_oper_platform 
               ,COUNT(*) OVER ( PARTITION BY opposing_id , mobile_oper_platform ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE mobile_oper_platform IS NOT NULL 
            AND opposing_id IS NOT NULL 
        )tmp
;


-- operation_channel
DROP TABLE IF EXISTS opposing_id_operation_channel_dist_cnt
;


CREATE TABLE opposing_id_operation_channel_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC ) AS opposing_id_operation_channel_dist_cnt 
   FROM (SELECT event_id 
               ,opposing_id 
               ,operation_channel 
               ,COUNT(*) OVER ( PARTITION BY opposing_id , operation_channel ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE operation_channel IS NOT NULL 
            AND opposing_id IS NOT NULL 
        )tmp
;


-- pay_scene
DROP TABLE IF EXISTS opposing_id_pay_scene_dist_cnt
;


CREATE TABLE opposing_id_pay_scene_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC ) AS opposing_id_pay_scene_dist_cnt 
   FROM (SELECT event_id 
               ,opposing_id 
               ,pay_scene 
               ,COUNT(*) OVER ( PARTITION BY opposing_id , pay_scene ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE pay_scene IS NOT NULL 
            AND opposing_id IS NOT NULL 
        )tmp
;


-- card_cert_no
DROP TABLE IF EXISTS opposing_id_card_cert_no_dist_cnt
;


CREATE TABLE opposing_id_card_cert_no_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC ) AS opposing_id_card_cert_no_dist_cnt 
   FROM (SELECT event_id 
               ,opposing_id 
               ,card_cert_no 
               ,COUNT(*) OVER ( PARTITION BY opposing_id , card_cert_no ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE card_cert_no IS NOT NULL 
            AND opposing_id IS NOT NULL 
        )tmp
;


-- ver
DROP TABLE IF EXISTS opposing_id_ver_dist_cnt
;


CREATE TABLE opposing_id_ver_dist_cnt AS
  SELECT event_id 
       ,SUM(CAST ( tmp_cnt = 1 AS INT )) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC ) AS opposing_id_ver_dist_cnt 
   FROM (SELECT event_id 
               ,opposing_id 
               ,ver 
               ,COUNT(*) OVER ( PARTITION BY opposing_id , ver ORDER BY ocu_date ASC ) AS tmp_cnt 
               ,ocu_date 
           FROM atec_all_data_series t1 
          WHERE ver IS NOT NULL 
            AND opposing_id IS NOT NULL 
        )tmp
;


DROP TABLE IF EXISTS opposing_id_dist_cnt_feature_cbc
;


CREATE TABLE opposing_id_dist_cnt_feature_cbc AS
  SELECT t1.event_id 
       ,opposing_id_user_id_dist_cnt 
       ,opposing_id_client_ip_dist_cnt 
       ,opposing_id_network_dist_cnt 
       ,opposing_id_device_sign_dist_cnt 
       ,opposing_id_ip_prov_dist_cnt 
       ,opposing_id_ip_city_dist_cnt 
       ,opposing_id_cert_prov_dist_cnt 
       ,opposing_id_cert_city_dist_cnt 
       ,opposing_id_card_mobile_prov_dist_cnt 
       ,opposing_id_card_mobile_city_dist_cnt 
       ,opposing_id_card_cert_prov_dist_cnt 
       ,opposing_id_card_cert_city_dist_cnt 
       ,opposing_id_mobile_oper_platform_dist_cnt 
       ,opposing_id_operation_channel_dist_cnt 
       ,opposing_id_pay_scene_dist_cnt 
       ,opposing_id_card_cert_no_dist_cnt 
       ,opposing_id_ver_dist_cnt 
   FROM (SELECT event_id 
           FROM atec_all_data_series 
        )t1 
 LEFT OUTER JOIN opposing_id_user_id_dist_cnt 
     ON opposing_id_user_id_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN opposing_id_client_ip_dist_cnt 
     ON opposing_id_client_ip_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN opposing_id_network_dist_cnt 
     ON opposing_id_network_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN opposing_id_device_sign_dist_cnt 
     ON opposing_id_device_sign_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN opposing_id_ip_prov_dist_cnt 
     ON opposing_id_ip_prov_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN opposing_id_ip_city_dist_cnt 
     ON opposing_id_ip_city_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN opposing_id_cert_prov_dist_cnt 
     ON opposing_id_cert_prov_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN opposing_id_cert_city_dist_cnt 
     ON opposing_id_cert_city_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN opposing_id_card_mobile_prov_dist_cnt 
     ON opposing_id_card_mobile_prov_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN opposing_id_card_mobile_city_dist_cnt 
     ON opposing_id_card_mobile_city_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN opposing_id_card_cert_prov_dist_cnt 
     ON opposing_id_card_cert_prov_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN opposing_id_card_cert_city_dist_cnt 
     ON opposing_id_card_cert_city_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN opposing_id_mobile_oper_platform_dist_cnt 
     ON opposing_id_mobile_oper_platform_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN opposing_id_operation_channel_dist_cnt 
     ON opposing_id_operation_channel_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN opposing_id_pay_scene_dist_cnt 
     ON opposing_id_pay_scene_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN opposing_id_card_cert_no_dist_cnt 
     ON opposing_id_card_cert_no_dist_cnt.event_id = t1.event_id 
 LEFT OUTER JOIN opposing_id_ver_dist_cnt 
     ON opposing_id_ver_dist_cnt.event_id = t1.event_id
;


