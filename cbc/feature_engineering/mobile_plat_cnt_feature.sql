-- mobile_oper_platform 各个历史窗口的频次特征

--创建统计表
--统计每天 platform 的出现次数
DROP TABLE IF EXISTS plat_everyday_cnt_cbc
;


CREATE TABLE plat_everyday_cnt_cbc AS
  SELECT mobile_oper_platform 
       ,year 
       ,month 
       ,day 
       ,COUNT(1)             AS plat_num_day 
   FROM atec_all_data_series 
  WHERE mobile_oper_platform IS NOT NULL 
 GROUP BY mobile_oper_platform 
         ,year 
         ,month 
         ,day
;


--得到上一天 platform 出现的次数
DROP TABLE IF EXISTS plat_lastday_cnt_cbc
;


CREATE TABLE plat_lastday_cnt_cbc AS
  SELECT mobile_oper_platform 
       ,year 
       ,month 
       ,day 
       ,LAG(plat_num_day ,1) OVER ( PARTITION BY mobile_oper_platform ORDER BY year , month , day ) AS plat_num_lag1day 
   FROM plat_everyday_cnt_cbc
;


INSERT OVERWRITE TABLE plat_lastday_cnt_cbc 
SELECT mobile_oper_platform 
      ,year 
      ,month 
      ,day 
      ,CASE WHEN plat_num_lag1day IS NOT NULL THEN plat_num_lag1day 
            WHEN plat_num_lag1day IS NULL THEN 0 
        END                 AS plat_num_lag1day 
  FROM plat_lastday_cnt_cbc
;


--得到历史平均出现次数
DROP TABLE IF EXISTS plat_cnt_day_avg_cbc
;


CREATE TABLE plat_cnt_day_avg_cbc AS
  SELECT mobile_oper_platform 
       ,month 
       ,day 
       ,AVG(plat_num_lag1day) OVER ( PARTITION BY mobile_oper_platform ORDER BY year , month , day ) AS plat_num_lag1day_avg 
   FROM plat_lastday_cnt_cbc
;


--合并原始表
DROP TABLE IF EXISTS plat_num_lag1day_avg_cbc
;


CREATE TABLE plat_num_lag1day_avg_cbc AS
  SELECT a.event_id 
       ,b.plat_num_lag1day_avg 
   FROM atec_all_data_series a 
 LEFT OUTER JOIN plat_cnt_day_avg_cbc b 
     ON a.mobile_oper_platform = b.mobile_oper_platform 
    AND a.month = b.month 
    AND a.day = b.day
;


--========创建 mobile_oper_platform 小时统计特征=========
DROP TABLE IF EXISTS plat_hr_cnt_cbc
;


CREATE TABLE plat_hr_cnt_cbc AS
  SELECT mobile_oper_platform 
       ,ocu_date 
       ,COUNT(1)             AS plat_num_1hr 
   FROM atec_all_data_series 
  WHERE mobile_oper_platform IS NOT NULL 
 GROUP BY mobile_oper_platform 
         ,ocu_date
;


DROP TABLE IF EXISTS plat_hr_cnt_avg_cbc
;

-- 小时平均特征
CREATE TABLE plat_hr_cnt_avg_cbc AS
  SELECT mobile_oper_platform 
       ,ocu_date 
       ,plat_num_1hr 
       ,AVG(plat_num_1hr) OVER ( PARTITION BY mobile_oper_platform ORDER BY ocu_date ) AS plat_num_1hr_avg 
   FROM plat_hr_cnt_cbc
;


--合并原始表
DROP TABLE IF EXISTS plat_cnt_hr_feature_cbc
;


CREATE TABLE plat_cnt_hr_feature_cbc AS
  SELECT a.event_id 
       ,b.plat_num_1hr 
       ,b.plat_num_1hr_avg 
   FROM atec_all_data_series a 
 LEFT OUTER JOIN plat_hr_cnt_avg_cbc b 
     ON a.mobile_oper_platform = b.mobile_oper_platform 
    AND a.ocu_date = b.ocu_date
;


--========历史天里的真实平均次数============
--mobile_oper_platform 的历史频次总数（前一天开始计算）
DROP TABLE IF EXISTS plat_lag1day_sum_cbc
;


CREATE TABLE plat_lag1day_sum_cbc AS
  SELECT t.mobile_oper_platform 
       ,t.year 
       ,t.month 
       ,t.day 
       ,t.day_num 
       ,SUM(t.plat_num_lag1day) OVER ( PARTITION BY t.mobile_oper_platform ORDER BY t.year , t.month , t.day ) AS plat_lag1day_sum 
   FROM (SELECT * 
               ,CASE WHEN month = 9 THEN day-4 
                     WHEN month = 10 THEN ( day+26 
                                          ) 
                     WHEN month = 11 THEN ( day+57 
                                          ) 
--                      WHEN month = 1 THEN day-4 
                     WHEN month = 2 THEN day-5 
                     WHEN month = 3 THEN ( day+23
                                         ) 
                 END AS day_num 
           FROM plat_lastday_cnt_cbc 
        )t
;


DROP TABLE IF EXISTS plat_day_avg_real_cbc
;

--mobile_oper_platform 的天平均频次
CREATE TABLE plat_day_avg_real_cbc AS
  SELECT mobile_oper_platform 
       ,year 
       ,month 
       ,day 
       ,( plat_lag1day_sum / day_num 
        )                    AS plat_lag1day_avg_real 
   FROM plat_lag1day_sum_cbc
;


DROP TABLE IF EXISTS plat_day_avg_real_feature_cbc
;

-- 合并表
CREATE TABLE plat_day_avg_real_feature_cbc AS
  SELECT a.event_id 
       ,b.plat_lag1day_avg_real 
   FROM atec_all_data_series a 
 LEFT OUTER JOIN plat_day_avg_real_cbc b 
     ON a.mobile_oper_platform = b.mobile_oper_platform 
    AND a.month = b.month 
    AND a.day = b.day
;


--========提取 mobile_oper_platform 的小时频次特征=======
DROP TABLE IF EXISTS plat_hr_sum_cbc
;

-- mobile_oper_platform 的历史累计频次（当前小时开始计算）
CREATE TABLE plat_hr_sum_cbc AS
  SELECT t.mobile_oper_platform 
       ,t.year 
       ,t.month 
       ,t.day 
       ,t.ocu_hr 
       ,SUM(t.plat_freq_deal_1hr) OVER ( PARTITION BY t.mobile_oper_platform ORDER BY t.year , t.month , t.day , t.ocu_hr ) AS plat_freq_deal_1hr_sum 
       ,CASE WHEN t.month = 9 THEN ( t.day-1-4 
                                   ) * 24+t.ocu_hr+1 
             WHEN t.month = 10 THEN ( t.day+30-1-4 
                                    ) * 24+t.ocu_hr+1 
             WHEN t.month = 11 THEN ( t.day+61-1-4 
                                    ) * 24+t.ocu_hr+1 
--              WHEN t.month = 1 THEN ( t.day-1-4 
--                                    ) * 24+t.ocu_hr+1 
             WHEN t.month = 2 THEN ( t.day-6 
                                   ) * 24+t.ocu_hr+1 
             WHEN t.month = 3 THEN ( t.day+28-6 
                                   ) * 24+t.ocu_hr+1 
         END                   AS hr_num 
   FROM (SELECT mobile_oper_platform 
               ,year 
               ,month 
               ,day 
               ,ocu_hr 
               ,COUNT(1)             AS plat_freq_deal_1hr 
           FROM atec_all_data_series 
          WHERE mobile_oper_platform IS NOT NULL 
         GROUP BY mobile_oper_platform 
                 ,year 
                 ,month 
                 ,day 
                 ,ocu_hr 
        )t
;


DROP TABLE IF EXISTS plat_hr_avg_real_cbc
;

-- 小时平均频次
CREATE TABLE plat_hr_avg_real_cbc AS
  SELECT a.event_id 
       ,b.plat_freq_deal_1hr_avg_real 
   FROM atec_all_data_series a 
 LEFT OUTER JOIN (SELECT mobile_oper_platform 
                        ,year 
                        ,month 
                        ,day 
                        ,ocu_hr 
                        ,( plat_freq_deal_1hr_sum / hr_num 
                         )                    AS plat_freq_deal_1hr_avg_real 
                    FROM plat_hr_sum_cbc 
                 )b 
     ON a.mobile_oper_platform = b.mobile_oper_platform 
    AND a.year = b.year 
    AND a.month = b.month 
    AND a.day = b.day 
    AND a.ocu_hr = b.ocu_hr
;


INSERT OVERWRITE TABLE plat_hr_avg_real_cbc 
SELECT event_id 
      ,CASE WHEN plat_freq_deal_1hr_avg_real IS NULL THEN 0 
            WHEN plat_freq_deal_1hr_avg_real IS NOT NULL THEN plat_freq_deal_1hr_avg_real 
        END AS plat_freq_deal_1hr_avg_real 
  FROM plat_hr_avg_real_cbc
;


--合并特征----
DROP TABLE IF EXISTS mobile_plat_cnt_feature_cbc
;


CREATE TABLE mobile_plat_cnt_feature_cbc AS
  SELECT a.event_id 
       ,a.plat_num_lag1day_avg 
       ,b.plat_num_1hr 
       ,b.plat_num_1hr_avg 
       ,c.plat_lag1day_avg_real 
       ,d.plat_freq_deal_1hr_avg_real 
   FROM plat_num_lag1day_avg_cbc a 
 LEFT OUTER JOIN plat_cnt_hr_feature_cbc b 
     ON a.event_id = b.event_id 
 LEFT OUTER JOIN plat_day_avg_real_feature_cbc c 
     ON a.event_id = c.event_id 
 LEFT OUTER JOIN plat_hr_avg_real_cbc d 
     ON a.event_id = d.event_id
;


