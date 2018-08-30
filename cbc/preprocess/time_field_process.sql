-- 提取时间特征

--==========train数据处理============
DROP TABLE IF EXISTS atec_train_data
;


CREATE TABLE atec_train_data AS
  SELECT * 
   FROM atec_1000w_ins_data
;


-- 拆分时间列
DROP TABLE IF EXISTS atec_train_data_1
;


CREATE TABLE atec_train_data_1 AS
  SELECT * 
       ,SUBSTR(gmt_occur ,1 ,10)  AS ocu_date 
       ,SUBSTR(gmt_occur ,12 ,13) AS ocu_hr 
   FROM atec_train_data
;


-- 创建事件时间表
DROP TABLE IF EXISTS event_time_train
;


CREATE TABLE event_time_train AS
  SELECT event_id 
       ,CAST ( SPLIT_PART(ocu_date ,'-' ,1) AS BIGINT ) AS year 
       ,CAST ( SPLIT_PART(ocu_date ,'-' ,2) AS BIGINT ) AS month 
       ,CAST ( SPLIT_PART(ocu_date ,'-' ,3) AS BIGINT ) AS day 
       ,TO_DATE(gmt_occur ,'yyyy-mm-dd hh')             AS ocu_date 
       ,CAST ( ocu_hr AS BIGINT )                       AS ocu_hr 
   FROM atec_train_data_1
;


--拿到序列化后的train数据
DROP TABLE IF EXISTS atec_train_series
;


CREATE TABLE atec_train_series AS
  SELECT a.* 
       ,b.ocu_date 
       ,b.ocu_hr 
       ,b.year 
       ,b.month 
       ,b.day 
   FROM lbenc_data a 
 INNER JOIN event_time_train b 
     ON a.event_id = b.event_id
;


--========== test 数据处理==================
DROP TABLE IF EXISTS atec_test_data
;


CREATE TABLE atec_test_data AS
  SELECT * 
   FROM atec_1000w_ootb_data
;


-- 拆分时间列
DROP TABLE IF EXISTS atec_test_data_1
;


CREATE TABLE atec_test_data_1 AS
  SELECT * 
       ,SUBSTR(gmt_occur ,1 ,10)  AS ocu_date 
       ,SUBSTR(gmt_occur ,12 ,13) AS ocu_hr 
   FROM atec_test_data
;


-- 创建事件时间表
DROP TABLE IF EXISTS event_time_test
;


CREATE TABLE event_time_test AS
  SELECT event_id 
       ,CAST ( SPLIT_PART(ocu_date ,'-' ,1) AS BIGINT ) AS year 
       ,CAST ( SPLIT_PART(ocu_date ,'-' ,2) AS BIGINT ) AS month 
       ,CAST ( SPLIT_PART(ocu_date ,'-' ,3) AS BIGINT ) AS day 
       ,TO_DATE(gmt_occur ,'yyyy-mm-dd hh')             AS ocu_date 
       ,CAST ( ocu_hr AS BIGINT )                       AS ocu_hr 
   FROM atec_test_data_1
;


--获得test集的序列化后的数据集
DROP TABLE IF EXISTS atec_test_series
;


CREATE TABLE atec_test_series AS
  SELECT a.* 
       ,b.ocu_date 
       ,b.ocu_hr 
       ,b.year 
       ,b.month 
       ,b.day 
   FROM lbenc_data a 
 INNER JOIN event_time_test b 
     ON a.event_id = b.event_id
;


--merge all data
DROP TABLE IF EXISTS atec_all_data_series
;


CREATE TABLE atec_all_data_series AS
  SELECT * 
   FROM (SELECT * 
               ,WEEKDAY(a.ocu_date) AS ocu_weekday 
           FROM atec_train_series a 
         UNION ALL 
         SELECT * 
               ,WEEKDAY(b.ocu_date) AS ocu_weekday 
           FROM atec_test_series b 
        )tmp
;


