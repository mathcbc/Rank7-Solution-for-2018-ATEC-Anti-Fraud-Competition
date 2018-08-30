-- 付款与收款的交叉特征

-- 本次交易前
DROP TABLE IF EXISTS tmp_payee_pay_hist_feat_cbc
;


CREATE TABLE tmp_payee_pay_hist_feat_cbc AS
  SELECT event_id 
       ,COUNT(1) AS payee_pay_num     -- 收款方本次交易前的付款次数
       ,AVG(amt) AS payee_pay_amt_avg -- 收款方本次交易前的付款平均额度
   FROM (SELECT t2.amt 
               ,t1.event_id 
           FROM atec_all_data_series t1 
         INNER JOIN atec_all_data_series t2 
             ON t1.opposing_id = t2.user_id 
          WHERE t1.ocu_date >= t2.ocu_date 
        )tmp 
 GROUP BY event_id
;


-- 一小时内
DROP TABLE IF EXISTS tmp_payee_pay_1hr_feat_cbc
;


CREATE TABLE tmp_payee_pay_1hr_feat_cbc AS
  SELECT event_id 
       ,COUNT(1) AS payee_pay_num_1hr     -- 收款方一小时内的付款次数
       ,AVG(amt) AS payee_pay_amt_avg_1hr -- 收款方一小时内的付款平均额度
   FROM (SELECT t2.amt 
               ,t1.event_id 
           FROM atec_all_data_series t1 
         INNER JOIN atec_all_data_series t2 
             ON t1.opposing_id = t2.user_id 
          WHERE t1.ocu_date = t2.ocu_date 
        )tmp 
 GROUP BY event_id
;


DROP TABLE IF EXISTS tmp_payer_receive_1hr_feat_cbc
;


CREATE TABLE tmp_payer_receive_1hr_feat_cbc AS
  SELECT event_id 
       ,COUNT(1) AS payer_receive_num_1hr     -- 付款方一小时内的收款次数
       ,AVG(amt) AS payer_receive_amt_avg_1hr -- 付款方一小时内的收款平均额度
   FROM (SELECT t2.amt 
               ,t1.event_id 
           FROM atec_all_data_series t1 
         INNER JOIN atec_all_data_series t2 
             ON t1.user_id = t2.opposing_id 
          WHERE t1.ocu_date = t2.ocu_date 
        )tmp 
 GROUP BY event_id
;


-- 前一天
DROP TABLE IF EXISTS tmp_payee_pay_pre1day_feat_cbc
;


CREATE TABLE tmp_payee_pay_pre1day_feat_cbc AS
  SELECT event_id 
       ,COUNT(1) AS payee_pay_num_pre1day     -- 收款方前一天的付款次数
       ,AVG(amt) AS payee_pay_amt_avg_pre1day -- 收款方前一天的付款平均额度
   FROM (SELECT t2.amt 
               ,t1.event_id 
           FROM atec_all_data_series t1 
         INNER JOIN atec_all_data_series t2 
             ON t1.opposing_id = t2.user_id 
          WHERE t1.ocu_date >= t2.ocu_date 
            AND t1.ocu_date <= DATEADD(t2.ocu_date ,1, 'day') 
        )tmp 
 GROUP BY event_id
;


DROP TABLE IF EXISTS tmp_payer_receive_pre1day_feat_cbc
;


CREATE TABLE tmp_payer_receive_pre1day_feat_cbc AS
  SELECT event_id 
       ,COUNT(1) AS payer_receive_num_pre1day     -- 付款方前一天的收款次数
       ,AVG(amt) AS payer_receive_amt_avg_pre1day -- 付款方前一天的收款平均额度
   FROM (SELECT t2.amt 
               ,t1.event_id 
           FROM atec_all_data_series t1 
         INNER JOIN atec_all_data_series t2 
             ON t1.opposing_id = t2.user_id 
          WHERE t1.ocu_date >= t2.ocu_date 
            AND t1.ocu_date <= DATEADD(t2.ocu_date ,1, 'day') 
        )tmp 
 GROUP BY event_id
;


DROP TABLE IF EXISTS pay_receive_cross_feature_cbc
;


CREATE TABLE pay_receive_cross_feature_cbc AS
  SELECT t1.event_id 
       ,payee_pay_num 
       ,payee_pay_amt_avg 
       ,payee_pay_num_1hr 
       ,payee_pay_amt_avg_1hr 
       ,payer_receive_num_1hr 
       ,payer_receive_amt_avg_1hr 
       ,payee_pay_num_pre1day 
       ,payee_pay_amt_avg_pre1day 
       ,payer_receive_num_pre1day 
       ,payer_receive_amt_avg_pre1day 
   FROM atec_all_data_series t1 
 LEFT OUTER JOIN tmp_payee_pay_hist_feat_cbc t2 
     ON t1.event_id = t2.event_id 
 LEFT OUTER JOIN tmp_payee_pay_1hr_feat_cbc t3 
     ON t1.event_id = t3.event_id 
 LEFT OUTER JOIN tmp_payer_receive_1hr_feat_cbc t4 
     ON t1.event_id = t4.event_id 
 LEFT OUTER JOIN tmp_payee_pay_pre1day_feat_cbc t5 
     ON t1.event_id = t5.event_id 
 LEFT OUTER JOIN tmp_payer_receive_pre1day_feat_cbc t6 
     ON t1.event_id = t6.event_id
;


