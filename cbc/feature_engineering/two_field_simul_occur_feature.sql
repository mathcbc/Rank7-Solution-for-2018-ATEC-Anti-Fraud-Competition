-- 当前 user_id 与 client_ip 一小时内同时出现的次数
-- 当前 user_id 与 client_ip 历史时间同时出现的次数
DROP TABLE IF EXISTS uid_ip_simul_cnt_1hr_all_cbc
;


CREATE TABLE uid_ip_simul_cnt_1hr_all_cbc AS
  SELECT event_id 
       ,COUNT(*) OVER ( PARTITION BY user_id , client_ip , year , month , day , ocu_hr ) AS uid_ip_simul_cnt_1hr 
       ,COUNT(*) OVER ( PARTITION BY user_id , client_ip ORDER BY ocu_date ASC )         AS uid_ip_simul_cnt_all 
   FROM atec_all_data_series 
  WHERE client_ip IS NOT NULL
;


-- 当前 opposing_id 与 client_ip 一小时内同时出现的次数
-- 当前 opposing_id 与 client_ip 历史时间同时出现的次数
DROP TABLE IF EXISTS opid_ip_simul_cnt_1hr_all_cbc
;


CREATE TABLE opid_ip_simul_cnt_1hr_all_cbc AS
  SELECT event_id 
       ,COUNT(*) OVER ( PARTITION BY opposing_id , client_ip , year , month , day , ocu_hr ) AS opid_ip_simul_cnt_1hr 
       ,COUNT(*) OVER ( PARTITION BY opposing_id , client_ip ORDER BY ocu_date ASC )         AS opid_ip_simul_cnt_all 
   FROM atec_all_data_series 
  WHERE client_ip IS NOT NULL
;


-- 当前 device_sign 与 client_ip 一小时内同时出现的次数
-- 当前 device_sign 与 client_ip 历史时间同时出现的次数
DROP TABLE IF EXISTS device_ip_simul_cnt_1hr_all_cbc
;


CREATE TABLE device_ip_simul_cnt_1hr_all_cbc AS
  SELECT event_id 
       ,COUNT(*) OVER ( PARTITION BY device_sign , client_ip , year , month , day , ocu_hr ) AS device_ip_simul_cnt_1hr 
       ,COUNT(*) OVER ( PARTITION BY device_sign , client_ip ORDER BY ocu_date ASC )         AS device_ip_simul_cnt_all 
   FROM atec_all_data_series 
  WHERE client_ip IS NOT NULL 
    AND device_sign IS NOT NULL
;


-- 当前 device_sign 与 opposing_id 一小时内同时出现的次数
-- 当前 device_sign 与 opposing_id 历史时间同时出现的次数
DROP TABLE IF EXISTS device_opid_simul_cnt_1hr_all_cbc
;


CREATE TABLE device_opid_simul_cnt_1hr_all_cbc AS
  SELECT event_id 
       ,COUNT(*) OVER ( PARTITION BY device_sign , opposing_id , year , month , day , ocu_hr ) AS device_opid_simul_cnt_1hr 
       ,COUNT(*) OVER ( PARTITION BY device_sign , opposing_id ORDER BY ocu_date ASC )         AS device_opid_simul_cnt_all 
   FROM atec_all_data_series 
  WHERE device_sign IS NOT NULL
;


-- join features
DROP TABLE IF EXISTS two_field_simul_occur_feature_cbc
;


CREATE TABLE two_field_simul_occur_feature_cbc AS
  SELECT t1.event_id 
       ,uid_ip_simul_cnt_1hr 
       ,uid_ip_simul_cnt_all 
       ,opid_ip_simul_cnt_1hr 
       ,opid_ip_simul_cnt_all 
       ,device_ip_simul_cnt_1hr 
       ,device_ip_simul_cnt_all 
       ,device_opid_simul_cnt_1hr 
       ,device_opid_simul_cnt_all 
   FROM (SELECT event_id 
           FROM atec_all_data_series 
        )t1 
 LEFT OUTER JOIN uid_ip_simul_cnt_1hr_all_cbc t2 
     ON t1.event_id = t2.event_id 
 LEFT OUTER JOIN opid_ip_simul_cnt_1hr_all_cbc t3 
     ON t1.event_id = t3.event_id 
 LEFT OUTER JOIN device_ip_simul_cnt_1hr_all_cbc t4 
     ON t1.event_id = t4.event_id 
 LEFT OUTER JOIN device_opid_simul_cnt_1hr_all_cbc t5 
     ON t1.event_id = t5.event_id
;


