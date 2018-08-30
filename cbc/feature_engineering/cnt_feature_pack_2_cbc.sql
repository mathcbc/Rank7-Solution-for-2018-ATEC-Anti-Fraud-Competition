-- 二补频次特征
-- payee_get_cnt			收款方的收款次数（此交易前）
-- ip_1day_cnt				ip一天内出现的次数	
-- payee_1day_get_cnt		收款方一天内收款次数	
-- peer_pay_cnt				历史时间里代付的次数	
-- peer_pay_1day_cnt		一天里代付的次数	
-- peer_pay_1hr_cnt			一小时内代付的次数		
DROP TABLE IF EXISTS tmp_cnt_feature_pack_2_cbc
;


CREATE TABLE tmp_cnt_feature_pack_2_cbc AS
  SELECT event_id 
       ,COUNT(*) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC )                     AS payee_get_cnt 
       ,COUNT(1) OVER ( PARTITION BY client_ip , month , day ORDER BY ocu_date ASC )         AS ip_1day_cnt 
       ,COUNT(1) OVER ( PARTITION BY opposing_id , month , day ORDER BY ocu_date ASC )       AS payee_1day_get_cnt 
       ,SUM(is_peer_pay-1) OVER ( PARTITION BY user_id ORDER BY ocu_date ASC )               AS peer_pay_cnt 
       ,SUM(is_peer_pay-1) OVER ( PARTITION BY user_id , month , day ORDER BY ocu_date ASC ) AS peer_pay_1day_cnt 
       ,SUM(is_peer_pay-1) OVER ( PARTITION BY user_id , month , day , ocu_hr )              AS peer_pay_1hr_cnt 
   FROM atec_all_data_series
;


-- device_sign_1hr_cnt		设备ID一小时内出现的次数	
-- device_sign_1day_cnt		设备ID一天内出现的次数	
-- device_sign_cnt			设备ID历史出现的次数
DROP TABLE IF EXISTS tmp_device_sign_cnt_cbc
;


CREATE TABLE tmp_device_sign_cnt_cbc AS
  SELECT event_id 
       ,COUNT(1) OVER ( PARTITION BY device_sign , month , day , ocu_hr )              AS device_sign_1hr_cnt 
       ,COUNT(1) OVER ( PARTITION BY device_sign , month , day ORDER BY ocu_date ASC ) AS device_sign_1day_cnt 
       ,COUNT(1) OVER ( PARTITION BY device_sign ORDER BY ocu_date ASC )               AS device_sign_cnt 
   FROM atec_all_data_series 
  WHERE device_sign IS NOT NULL
;


-- cert_no_1hr_cnt			付款方证件号一小时内出现的次数	
-- cert_no_1day_cnt			付款方证件号一天内出现的次数	
-- cert_no_cnt				付款方证件号历史出现的次数
DROP TABLE IF EXISTS tmp_cert_no_cnt_cbc
;


CREATE TABLE tmp_cert_no_cnt_cbc AS
  SELECT event_id 
       ,COUNT(1) OVER ( PARTITION BY card_cert_no , month , day , ocu_hr )              AS cert_no_1hr_cnt 
       ,COUNT(1) OVER ( PARTITION BY card_cert_no , month , day ORDER BY ocu_date ASC ) AS cert_no_1day_cnt 
       ,COUNT(1) OVER ( PARTITION BY card_cert_no ORDER BY ocu_date ASC )               AS cert_no_cnt 
   FROM atec_all_data_series 
  WHERE card_cert_no IS NOT NULL
;


DROP TABLE IF EXISTS cnt_feature_pack_2_cbc
;


CREATE TABLE cnt_feature_pack_2_cbc AS
  SELECT t1.* 
       ,device_sign_1hr_cnt 
       ,device_sign_1day_cnt 
       ,device_sign_cnt 
       ,cert_no_1hr_cnt 
       ,cert_no_1day_cnt 
       ,cert_no_cnt 
   FROM tmp_cnt_feature_pack_2_cbc t1 
 LEFT OUTER JOIN tmp_device_sign_cnt_cbc t2 
     ON t1.event_id = t2.event_id 
 LEFT OUTER JOIN tmp_cert_no_cnt_cbc t3 
     ON t1.event_id = t3.event_id
;

drop TABLE if EXISTS tmp_cnt_feature_pack_2_cbc;
DROP TABLE if EXISTS tmp_device_sign_cnt_cbc;
DROP TABLE if EXISTS tmp_cert_no_cnt_cbc;
