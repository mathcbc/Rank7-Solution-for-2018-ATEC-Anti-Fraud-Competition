-- 合并训练集和测试集
DROP TABLE IF EXISTS train_cbc
;


DROP TABLE IF EXISTS test_cbc
;


DROP TABLE IF EXISTS data_all
;


CREATE TABLE train_cbc AS
  SELECT * 
   FROM atec_1000w_ins_data
;


CREATE TABLE test_cbc AS
  SELECT * 
       ,- 2 AS is_fraud 
   FROM atec_1000w_ootb_data
;


CREATE TABLE data_all AS
  SELECT * 
   FROM (SELECT * 
           FROM train_cbc 
         UNION ALL 
         SELECT * 
           FROM test_cbc 
        )tmp
;


ALTER TABLE data_all CHANGE COLUMN `version` ver STRING
;


