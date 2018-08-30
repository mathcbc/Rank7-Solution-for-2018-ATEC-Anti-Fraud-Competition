-- 前一天的频次特征

-- 生成日期的顺序（9月1日作为第一天）
DROP TABLE IF EXISTS tmp_all_data_day_rank_cbc
;


CREATE TABLE tmp_all_data_day_rank_cbc AS
  SELECT t1.* 
       ,CASE WHEN month = 9 THEN day 
             WHEN month = 10 THEN day+30 
             WHEN month = 11 THEN day+61 
             WHEN month = 1 THEN day+122 
             WHEN month = 2 THEN day+153 
             WHEN month = 3 THEN day+181 
         END AS day_rank 
   FROM atec_all_data_series t1
;


DROP TABLE IF EXISTS tmp_table_cbc
;


-- opposing_id 各天的出现频次
CREATE TABLE tmp_table_cbc AS
  SELECT opposing_id 
       ,day_rank 
       ,COUNT(1)    AS opposing_id_num_day 
   FROM tmp_all_data_day_rank_cbc 
 GROUP BY opposing_id 
         ,day_rank
;


--得到当前交易的上一天 opposing_id 出现的次数
DROP TABLE IF EXISTS tmp_opid_cnt_lastday
;


CREATE TABLE tmp_opid_cnt_lastday AS
  SELECT opposing_id 
       ,day_rank 
       ,CASE WHEN LAG(day_rank ,1) OVER ( PARTITION BY opposing_id ORDER BY day_rank ) != day_rank-1 THEN 0 
             ELSE LAG(opposing_id_num_day ,1) OVER ( PARTITION BY opposing_id ORDER BY day_rank ) 
         END        AS opposing_id_cnt_lastday 
   FROM tmp_table_cbc
;


DROP TABLE IF EXISTS tmp_table_cbc
;


-- user_id 各天的出现频次
CREATE TABLE tmp_table_cbc AS
  SELECT user_id 
       ,day_rank 
       ,COUNT(1) AS user_id_num_day 
   FROM tmp_all_data_day_rank_cbc 
 GROUP BY user_id 
         ,day_rank
;


--得到当前交易的上一天 user_id 出现的次数
DROP TABLE IF EXISTS tmp_uid_cnt_lastday
;


CREATE TABLE tmp_uid_cnt_lastday AS
  SELECT user_id 
       ,day_rank 
       ,CASE WHEN LAG(day_rank ,1) OVER ( PARTITION BY user_id ORDER BY day_rank ) != day_rank-1 THEN 0 
             ELSE LAG(user_id_num_day ,1) OVER ( PARTITION BY user_id ORDER BY day_rank ) 
         END AS user_id_cnt_lastday 
   FROM tmp_table_cbc
;


DROP TABLE IF EXISTS tmp_table_cbc
;


-- client_ip 各天的出现频次
CREATE TABLE tmp_table_cbc AS
  SELECT client_ip 
       ,day_rank 
       ,COUNT(1)  AS client_ip_num_day 
   FROM tmp_all_data_day_rank_cbc 
 GROUP BY client_ip 
         ,day_rank
;


--得到当前交易的上一天 client_ip 出现的次数
DROP TABLE IF EXISTS tmp_ip_cnt_lastday
;


CREATE TABLE tmp_ip_cnt_lastday AS
  SELECT client_ip 
       ,day_rank 
       ,CASE WHEN LAG(day_rank ,1) OVER ( PARTITION BY client_ip ORDER BY day_rank ) != day_rank-1 THEN 0 
             ELSE LAG(client_ip_num_day ,1) OVER ( PARTITION BY client_ip ORDER BY day_rank ) 
         END      AS client_ip_cnt_lastday 
   FROM tmp_table_cbc
;


DROP TABLE IF EXISTS tmp_table_cbc
;


-- device_sign 各天的出现频次
CREATE TABLE tmp_table_cbc AS
  SELECT device_sign 
       ,day_rank 
       ,COUNT(1)    AS device_sign_num_day 
   FROM tmp_all_data_day_rank_cbc 
 GROUP BY device_sign 
         ,day_rank
;


--得到当前交易的上一天 device_sign 出现的次数
DROP TABLE IF EXISTS tmp_device_cnt_lastday
;


CREATE TABLE tmp_device_cnt_lastday AS
  SELECT device_sign 
       ,day_rank 
       ,CASE WHEN LAG(day_rank ,1) OVER ( PARTITION BY device_sign ORDER BY day_rank ) != day_rank-1 THEN 0 
             ELSE LAG(device_sign_num_day ,1) OVER ( PARTITION BY device_sign ORDER BY day_rank ) 
         END        AS device_sign_cnt_lastday 
   FROM tmp_table_cbc
;


DROP TABLE IF EXISTS tmp_table_cbc
;


-- card_cert_no 各天的出现频次
CREATE TABLE tmp_table_cbc AS
  SELECT card_cert_no 
       ,day_rank 
       ,COUNT(1)     AS card_cert_no_num_day 
   FROM tmp_all_data_day_rank_cbc 
 GROUP BY card_cert_no 
         ,day_rank
;


--得到当前交易的上一天 card_cert_no 出现的次数
DROP TABLE IF EXISTS tmp_card_cert_no_cnt_lastday
;


CREATE TABLE tmp_card_cert_no_cnt_lastday AS
  SELECT card_cert_no 
       ,day_rank 
       ,CASE WHEN LAG(day_rank ,1) OVER ( PARTITION BY card_cert_no ORDER BY day_rank ) != day_rank-1 THEN 0 
             ELSE LAG(card_cert_no_num_day ,1) OVER ( PARTITION BY card_cert_no ORDER BY day_rank ) 
         END         AS card_cert_no_cnt_lastday 
   FROM tmp_table_cbc
;


-- join table
DROP TABLE IF EXISTS lastday_cnt_feature_cbc
;


CREATE TABLE lastday_cnt_feature_cbc AS
  SELECT t1.event_id 
       ,opposing_id_cnt_lastday 
       ,user_id_cnt_lastday 
       ,client_ip_cnt_lastday 
       ,device_sign_cnt_lastday 
       ,card_cert_no_cnt_lastday 
   FROM tmp_all_data_day_rank_cbc t1 
 LEFT OUTER JOIN tmp_opid_cnt_lastday t2 
     ON t1.opposing_id = t2.opposing_id 
    AND t1.day_rank = t2.day_rank 
 LEFT OUTER JOIN tmp_uid_cnt_lastday t3 
     ON t1.user_id = t3.user_id 
    AND t1.day_rank = t3.day_rank 
 LEFT OUTER JOIN tmp_device_cnt_lastday t4 
     ON t1.device_sign = t4.device_sign 
    AND t1.day_rank = t4.day_rank 
 LEFT OUTER JOIN tmp_card_cert_no_cnt_lastday t5 
     ON t1.card_cert_no = t5.card_cert_no 
    AND t1.day_rank = t5.day_rank 
 LEFT OUTER JOIN tmp_ip_cnt_lastday t6 
     ON t1.client_ip = t6.client_ip 
    AND t1.day_rank = t6.day_rank
;


-- 删除中间表
DROP TABLE IF EXISTS tmp_all_data_day_rank_cbc
;


