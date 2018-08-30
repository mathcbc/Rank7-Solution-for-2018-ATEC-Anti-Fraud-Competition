--付款方当前小时的交易金额特征
-- payer_amt_sum_1hr	付款方一小时内消费总额	
-- payer_amt_avg_1hr	付款方一小时内平均金额	
-- payer_amt_max_1hr	付款方一小时内最大金额	
-- payer_amt_min_1hr	付款方一小时内最小金额	
-- amt_subtract_avg_1hr	付款方当前金额减一小时平均	
-- amt_subtract_max_1hr	付款方当前金额减一小时最大	
-- amt_subtract_min_1hr	付款方当前金额减一小时最小
DROP TABLE IF EXISTS tmp_payer_amt_1hr_feature
;


CREATE TABLE tmp_payer_amt_1hr_feature AS
  SELECT event_id 
       ,amt 
       ,SUM(amt) OVER ( PARTITION BY user_id , month , day , ocu_hr ) AS payer_amt_sum_1hr 
       ,AVG(amt) OVER ( PARTITION BY user_id , month , day , ocu_hr ) AS payer_amt_avg_1hr 
       ,MAX(amt) OVER ( PARTITION BY user_id , month , day , ocu_hr ) AS payer_amt_max_1hr 
       ,MIN(amt) OVER ( PARTITION BY user_id , month , day , ocu_hr ) AS payer_amt_min_1hr 
   FROM atec_all_data_series
;


DROP TABLE IF EXISTS payer_amt_1hr_feature
;


CREATE TABLE payer_amt_1hr_feature AS
  SELECT event_id 
       ,payer_amt_sum_1hr 
       ,payer_amt_avg_1hr 
       ,payer_amt_max_1hr 
       ,payer_amt_min_1hr 
       ,amt-payer_amt_avg_1hr AS amt_subtract_avg_1hr 
       ,amt-payer_amt_max_1hr AS amt_subtract_max_1hr 
       ,amt-payer_amt_min_1hr AS amt_subtract_min_1hr 
   FROM tmp_payer_amt_1hr_feature
;


DROP TABLE IF EXISTS tmp_payer_amt_1hr_feature
;


