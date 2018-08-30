-- 求历史所有相同时钟里的平均频次

-- 生成日期的顺序（训练集里9月5日作为第一天，测试集里1月5号作为第一天）
DROP TABLE IF EXISTS tmp_all_data_day_num_cbc
;


CREATE TABLE tmp_all_data_day_num_cbc AS
  SELECT t1.* 
       ,CASE WHEN month = 9 THEN day-4 
             WHEN month = 10 THEN day+26 
             WHEN month = 11 THEN day+57 
             WHEN month = 1 THEN day-4 
             WHEN month = 2 THEN day+27 
             WHEN month = 3 THEN day+55 
         END AS day_num 
       ,COUNT(*) OVER ( PARTITION BY user_id , ocu_hr ORDER BY ocu_date ASC )     AS uid_cnt_clock 
       ,COUNT(*) OVER ( PARTITION BY opposing_id , ocu_hr ORDER BY ocu_date ASC ) AS opid_cnt_clock 
       ,COUNT(*) OVER ( PARTITION BY client_ip , ocu_hr ORDER BY ocu_date ASC )   AS ip_cnt_clock 
       ,COUNT(*) OVER ( PARTITION BY device_sign , ocu_hr ORDER BY ocu_date ASC ) AS device_cnt_clock 
   FROM atec_all_data_series t1
;


DROP TABLE IF EXISTS tmp_clock_avg_cnt_cbc
;


CREATE TABLE tmp_clock_avg_cnt_cbc AS
  SELECT event_id 
       ,uid_cnt_clock / day_num    AS uid_cnt_clock_avg    --付款方历史所有相同时钟下的平均频次
       ,opid_cnt_clock / day_num   AS opid_cnt_clock_avg   --收款方历史所有相同时钟下的平均频次
       ,ip_cnt_clock / day_num     AS ip_cnt_clock_avg     --ip 历史所有相同时钟下的平均频次
       ,device_cnt_clock / day_num AS device_cnt_clock_avg --设备 ID 历史所有相同时钟下的平均频次
   FROM tmp_all_data_day_num_cbc
;


DROP TABLE IF EXISTS avg_cnt_clock_cbc
;


CREATE TABLE avg_cnt_clock_cbc AS
  SELECT t1.event_id 
       ,uid_cnt_clock_avg 
       ,opid_cnt_clock_avg 
       ,ip_cnt_clock_avg 
       ,device_cnt_clock_avg 
       ,uid_cnt_clock_avg-freq_deal_1hr          AS uid_avg_cnt_clock_subtract    -- user_id 减去当前小时频次
       ,opid_cnt_clock_avg-payee_1hr_get_cnt     AS opid_avg_cnt_clock_subtract   -- oposing_id 减去当前小时频次
       ,ip_cnt_clock_avg-ip_1hr_cnt              AS ip_avg_cnt_clock_subtract     -- ip 减去当前小时频次
       ,device_cnt_clock_avg-device_sign_1hr_cnt AS device_avg_cnt_clock_subtract -- device 减去当前小时频次
   FROM tmp_clock_avg_cnt_cbc t1 
 LEFT OUTER JOIN extra_cnt_featurce_cbc t2 
     ON t1.event_id = t2.event_id 
 LEFT OUTER JOIN cnt_feature_pack_2_cbc t3 
     ON t1.event_id = t3.event_id 
 LEFT OUTER JOIN atec_all_data_feature_10 t4 
     ON t1.event_id = t4.event_id
;


-- 删除中间表
DROP TABLE IF EXISTS tmp_all_data_day_num_cbc
;


DROP TABLE IF EXISTS tmp_clock_avg_cnt_cbc
;


