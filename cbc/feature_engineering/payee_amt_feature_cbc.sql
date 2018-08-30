--收款方的交易金额特征
-- payee_amt_total	收款方历史时间内收款总额	
-- payee_amt_avg	收款方历史时间收款平均值	
-- payee_amt_max	收款方历史时间收款最大值	
-- payee_amt_min	收款方历史时间收款最小值	
-- payee_amt_minus_avg	收款方金额减平均值	
-- payee_amt_minus_max	收款方金额减最大值	
-- payee_amt_minus_min	收款方金额减最小值
DROP TABLE IF EXISTS tmp_payee_amt_feature_cbc
;


CREATE TABLE tmp_payee_amt_feature_cbc AS
  SELECT event_id 
       ,amt 
       ,SUM(amt) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC ) AS payee_amt_total 
       ,AVG(amt) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC ) AS payee_amt_avg 
       ,MAX(amt) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC ) AS payee_amt_max 
       ,MIN(amt) OVER ( PARTITION BY opposing_id ORDER BY ocu_date ASC ) AS payee_amt_min 
   FROM atec_all_data_series
;


DROP TABLE IF EXISTS payee_amt_feature_cbc
;


CREATE TABLE payee_amt_feature_cbc AS
  SELECT event_id 
       ,payee_amt_total 
       ,payee_amt_avg 
       ,payee_amt_max 
       ,payee_amt_min 
       ,amt-payee_amt_avg AS payee_amt_minus_avg 
       ,amt-payee_amt_max AS payee_amt_minus_max 
       ,amt-payee_amt_min AS payee_amt_minus_min 
   FROM tmp_payee_amt_feature_cbc
;


DROP TABLE IF EXISTS tmp_payee_amt_feature_cbc
;


