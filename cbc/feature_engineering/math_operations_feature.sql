-- 对以下特征进行加法和乘法运算
-- avg_lag1_freq_deal_sum1day
-- freq_deal_1hr
-- user_id_lag1day_avg_real
-- opposing_id_num_1hr_avg 
-- device_sign_lag1day_avg_real
DROP TABLE IF EXISTS tmp_k_most_feature_cbc
;


CREATE TABLE tmp_k_most_feature_cbc AS
  SELECT t1.event_id 
       ,avg_lag1_freq_deal_sum1day   AS f1 
       ,freq_deal_1hr                AS f2 
       ,user_id_lag1day_avg_real     AS f3 
       ,opposing_id_num_1hr_avg      AS f4 
       ,device_sign_lag1day_avg_real AS f5 
   FROM atec_all_data_series t1 
 LEFT OUTER JOIN atec_all_data_feature_10 t2 
     ON t1.event_id = t2.event_id 
 LEFT OUTER JOIN atec_all_data_feature_22 t3 
     ON t1.event_id = t3.event_id 
 LEFT OUTER JOIN atec_all_data_feature_52 t4 
     ON t1.event_id = t4.event_id 
 LEFT OUTER JOIN atec_all_data_feature_82 t5 
     ON t1.event_id = t5.event_id
;


DROP TABLE IF EXISTS math_oper_feature_cbc
;


CREATE TABLE math_oper_feature_cbc AS
  SELECT event_id 
       ,f1+f2    AS f1_add_f2 
       ,f1 * f2  AS f1_mul_f2 
       ,f1+f3    AS f1_add_f3 
       ,f1 * f3  AS f1_mul_f3 
       ,f1+f4    AS f1_add_f4 
       ,f1 * f4  AS f1_mul_f4 
       ,f1+f5    AS f1_add_f5 
       ,f1 * f5  AS f1_mul_f5 
       ,f2+f3    AS f2_add_f3 
       ,f2 * f3  AS f2_mul_f3 
       ,f2+f4    AS f2_add_f4 
       ,f2 * f4  AS f2_mul_f4 
       ,f2+f5    AS f2_add_f5 
       ,f2 * f5  AS f2_mul_f5 
       ,f3+f4    AS f3_add_f4 
       ,f3 * f4  AS f3_mul_f4 
       ,f3+f5    AS f3_add_f5 
       ,f3 * f5  AS f3_mul_f5 
       ,f4+f5    AS f4_add_f5 
       ,f4 * f5  AS f4_mul_f5 
   FROM tmp_k_most_feature_cbc
;


