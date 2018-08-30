-- 两字段同时出现的字段以及地区相等的特征

-- user_id 与 opposing_id 一小时内的交易次数
DROP TABLE IF EXISTS uid_oppoid_pay_cnt_1hr_cbc
;


CREATE TABLE uid_oppoid_pay_cnt_1hr_cbc AS
  SELECT event_id 
       ,COUNT(*) OVER ( PARTITION BY user_id , opposing_id , year , month , day , ocu_hr ) AS uid_opid_cnt_1hr 
   FROM atec_all_data_series
;


-- user_id 与 opposing_id 历史时间的交易次数
DROP TABLE IF EXISTS uid_oppoid_pay_cnt_all_cbc
;


CREATE TABLE uid_oppoid_pay_cnt_all_cbc AS
  SELECT event_id 
       ,COUNT(*) OVER ( PARTITION BY user_id , opposing_id ORDER BY ocu_date ASC ) AS uid_opid_cnt_all 
   FROM atec_all_data_series
;


-- 当前 user_id 与 opposing_id 前一天的交易次数
DROP TABLE IF EXISTS uid_opid_1day_pre_cnt
;


CREATE TABLE uid_opid_1day_pre_cnt AS
  SELECT user_id 
       ,opposing_id 
       ,year 
       ,month 
       ,day 
       ,LAG(tmp_cnt ,1) OVER ( PARTITION BY user_id , opposing_id ORDER BY year , month , day ASC ) AS uid_opid_cnt_1day_pre 
   FROM (SELECT user_id 
               ,opposing_id 
               ,year 
               ,month 
               ,day 
               ,COUNT(*)    AS tmp_cnt 
           FROM atec_all_data_series 
         GROUP BY user_id 
                 ,opposing_id 
                 ,year 
                 ,month 
                 ,day 
        )tmp
;


CREATE TABLE uid_opid_1day_pre_cnt_feature_cbc AS
  SELECT t1.event_id 
       ,CASE WHEN uid_opid_cnt_1day_pre IS NOT NULL THEN uid_opid_cnt_1day_pre -- 当前 user_id 与 opposing_id 前一天的交易次数
             WHEN uid_opid_cnt_1day_pre IS NULL THEN 0 
         END        AS uid_opid_cnt_1day_pre 
   FROM atec_all_data_series t1 
 LEFT OUTER JOIN uid_opid_1day_pre_cnt t2 
     ON t1.user_id = t2.user_id 
    AND t1.opposing_id = t2.opposing_id 
    AND t1.year = t2.year 
    AND t1.month = t2.month 
    AND t1.day = t2.day
;


-- 合并特征
DROP TABLE IF EXISTS feature_0803_cbc
;


CREATE TABLE feature_0803_cbc AS
  SELECT t1.event_id 
       ,CAST ( ip_prov = cert_prov AS INT )        AS ip_prov_equal_cert_prov   --ip省等于证件省	
       ,CAST ( ip_prov = card_mobile_prov AS INT ) AS ip_prov_equal_mobile_prov --ip省等于手机账号省
       ,CAST ( ip_prov = card_cert_prov AS INT )   AS ip_prov_equal_card_prov   --ip省等于银行卡省
       ,CAST ( ip_city = cert_city AS INT )        AS ip_city_equal_cert_city   --ip市等于证件市
       ,CAST ( ip_city = card_mobile_city AS INT ) AS ip_city_equal_mobile_city --ip市等于手机账号市
       ,CAST ( ip_city = card_cert_city AS INT )   AS ip_city_equal_card_city   --ip市等于银行卡市
       ,uid_opid_cnt_1hr -- user_id 与 opposing_id 一小时内的交易次数
       ,uid_opid_cnt_all -- user_id 与 opposing_id 历史时间的交易次数
       ,uid_opid_cnt_1day_pre -- 当前 user_id 与 opposing_id 前一天的交易次数
   FROM atec_all_data_series t1 
 LEFT OUTER JOIN uid_oppoid_pay_cnt_1hr_cbc t2 
     ON t1.event_id = t2.event_id 
 LEFT OUTER JOIN uid_oppoid_pay_cnt_all_cbc t3 
     ON t1.event_id = t3.event_id 
 LEFT OUTER JOIN uid_opid_1day_pre_cnt_feature_cbc t4 
     ON t1.event_id = t4.event_id
;


DROP TABLE IF EXISTS uid_oppoid_pay_cnt_1hr_cbc
;


DROP TABLE IF EXISTS uid_oppoid_pay_cnt_all_cbc
;


DROP TABLE IF EXISTS uid_opid_1day_pre_cnt_feature_cbc
;


DROP TABLE IF EXISTS uid_opid_1day_pre_cnt
;


