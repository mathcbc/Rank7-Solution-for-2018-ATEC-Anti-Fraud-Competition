-- 数据编码：将 string 类型字段编码为 int 型
DROP TABLE IF EXISTS lbenc_data
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE lbenc_data AS
  SELECT event_id 
       ,amt 
       ,is_fraud 
   FROM data_all
;


-- 构建用户ID的编码表
DROP TABLE IF EXISTS tmp_table
;


CREATE TABLE tmp_table AS
  SELECT * 
   FROM (SELECT user_id AS tmp_name 
           FROM data_all 
         UNION ALL 
         SELECT opposing_id AS tmp_name 
           FROM data_all 
        )tmp
;


DROP TABLE IF EXISTS tmp_rownum_table
;


CREATE TABLE tmp_rownum_table AS
  SELECT tmp_name 
       ,ROW_NUMBER() OVER ( ORDER BY tmp_name ) AS enc 
   FROM tmp_table 
  WHERE tmp_name IS NOT NULL 
 GROUP BY tmp_name
;


DROP TABLE IF EXISTS tmp_table
;


-- user_id:
DROP TABLE IF EXISTS user_id_enc_table
;


CREATE TABLE user_id_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.enc     AS user_id 
   FROM tmp_rownum_table enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.user_id = enc_t.tmp_name
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( user_id_enc_table ) */ 
       lbenc_data.* 
       ,user_id 
   FROM lbenc_data 
 LEFT OUTER JOIN user_id_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS user_id_enc_table
;


-- opposing_id:
DROP TABLE IF EXISTS opposing_id_enc_table
;


CREATE TABLE opposing_id_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.enc     AS opposing_id 
   FROM tmp_rownum_table enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.opposing_id = enc_t.tmp_name
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( opposing_id_enc_table ) */ 
       lbenc_data.* 
       ,opposing_id 
   FROM lbenc_data 
 LEFT OUTER JOIN opposing_id_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS opposing_id_enc_table
;


-- 构建省份字段的编码表
DROP TABLE IF EXISTS tmp_table
;


CREATE TABLE tmp_table AS
  SELECT * 
   FROM (SELECT ip_prov AS tmp_name 
           FROM data_all 
         UNION ALL 
         SELECT cert_prov AS tmp_name 
           FROM data_all 
         UNION ALL 
         SELECT card_bin_prov AS tmp_name 
           FROM data_all 
         UNION ALL 
         SELECT card_mobile_prov AS tmp_name 
           FROM data_all 
         UNION ALL 
         SELECT card_cert_prov AS tmp_name 
           FROM data_all 
         UNION ALL 
         SELECT province AS tmp_name 
           FROM data_all 
        )tmp
;


DROP TABLE IF EXISTS tmp_rownum_table
;


CREATE TABLE tmp_rownum_table AS
  SELECT tmp_name 
       ,ROW_NUMBER() OVER ( ORDER BY tmp_name ) AS enc 
   FROM tmp_table 
  WHERE tmp_name IS NOT NULL 
 GROUP BY tmp_name
;


DROP TABLE IF EXISTS tmp_table
;


-- ip_prov:
DROP TABLE IF EXISTS ip_prov_enc_table
;


CREATE TABLE ip_prov_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.enc     AS ip_prov 
   FROM tmp_rownum_table enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.ip_prov = enc_t.tmp_name
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( ip_prov_enc_table ) */ 
       lbenc_data.* 
       ,ip_prov 
   FROM lbenc_data 
 LEFT OUTER JOIN ip_prov_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS ip_prov_enc_table
;


-- cert_prov:
DROP TABLE IF EXISTS cert_prov_enc_table
;


CREATE TABLE cert_prov_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.enc     AS cert_prov 
   FROM tmp_rownum_table enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.cert_prov = enc_t.tmp_name
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( cert_prov_enc_table ) */ 
       lbenc_data.* 
       ,cert_prov 
   FROM lbenc_data 
 LEFT OUTER JOIN cert_prov_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS cert_prov_enc_table
;


-- card_bin_prov:
DROP TABLE IF EXISTS card_bin_prov_enc_table
;


CREATE TABLE card_bin_prov_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.enc     AS card_bin_prov 
   FROM tmp_rownum_table enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.card_bin_prov = enc_t.tmp_name
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( card_bin_prov_enc_table ) */ 
       lbenc_data.* 
       ,card_bin_prov 
   FROM lbenc_data 
 LEFT OUTER JOIN card_bin_prov_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS card_bin_prov_enc_table
;


-- card_mobile_prov:
DROP TABLE IF EXISTS card_mobile_prov_enc_table
;


CREATE TABLE card_mobile_prov_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.enc     AS card_mobile_prov 
   FROM tmp_rownum_table enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.card_mobile_prov = enc_t.tmp_name
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( card_mobile_prov_enc_table ) */ 
       lbenc_data.* 
       ,card_mobile_prov 
   FROM lbenc_data 
 LEFT OUTER JOIN card_mobile_prov_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS card_mobile_prov_enc_table
;


-- card_cert_prov:
DROP TABLE IF EXISTS card_cert_prov_enc_table
;


CREATE TABLE card_cert_prov_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.enc     AS card_cert_prov 
   FROM tmp_rownum_table enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.card_cert_prov = enc_t.tmp_name
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( card_cert_prov_enc_table ) */ 
       lbenc_data.* 
       ,card_cert_prov 
   FROM lbenc_data 
 LEFT OUTER JOIN card_cert_prov_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS card_cert_prov_enc_table
;


-- province:
DROP TABLE IF EXISTS province_enc_table
;


CREATE TABLE province_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.enc     AS province 
   FROM tmp_rownum_table enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.province = enc_t.tmp_name
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( province_enc_table ) */ 
       lbenc_data.* 
       ,province 
   FROM lbenc_data 
 LEFT OUTER JOIN province_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS province_enc_table
;


-- 构建城市字段的编码表
DROP TABLE IF EXISTS tmp_table
;


CREATE TABLE tmp_table AS
  SELECT * 
   FROM (SELECT ip_city AS tmp_name 
           FROM data_all 
         UNION ALL 
         SELECT cert_city AS tmp_name 
           FROM data_all 
         UNION ALL 
         SELECT card_bin_city AS tmp_name 
           FROM data_all 
         UNION ALL 
         SELECT card_mobile_city AS tmp_name 
           FROM data_all 
         UNION ALL 
         SELECT card_cert_city AS tmp_name 
           FROM data_all 
         UNION ALL 
         SELECT city AS tmp_name 
           FROM data_all 
        )tmp
;


DROP TABLE IF EXISTS tmp_rownum_table
;


CREATE TABLE tmp_rownum_table AS
  SELECT tmp_name 
       ,ROW_NUMBER() OVER ( ORDER BY tmp_name ) AS enc 
   FROM tmp_table 
  WHERE tmp_name IS NOT NULL 
 GROUP BY tmp_name
;


DROP TABLE IF EXISTS tmp_table
;


-- ip_city:
DROP TABLE IF EXISTS ip_city_enc_table
;


CREATE TABLE ip_city_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.enc     AS ip_city 
   FROM tmp_rownum_table enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.ip_city = enc_t.tmp_name
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( ip_city_enc_table ) */ 
       lbenc_data.* 
       ,ip_city 
   FROM lbenc_data 
 LEFT OUTER JOIN ip_city_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS ip_city_enc_table
;


-- cert_city:
DROP TABLE IF EXISTS cert_city_enc_table
;


CREATE TABLE cert_city_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.enc     AS cert_city 
   FROM tmp_rownum_table enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.cert_city = enc_t.tmp_name
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( cert_city_enc_table ) */ 
       lbenc_data.* 
       ,cert_city 
   FROM lbenc_data 
 LEFT OUTER JOIN cert_city_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS cert_city_enc_table
;


-- card_bin_city:
DROP TABLE IF EXISTS card_bin_city_enc_table
;


CREATE TABLE card_bin_city_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.enc     AS card_bin_city 
   FROM tmp_rownum_table enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.card_bin_city = enc_t.tmp_name
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( card_bin_city_enc_table ) */ 
       lbenc_data.* 
       ,card_bin_city 
   FROM lbenc_data 
 LEFT OUTER JOIN card_bin_city_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS card_bin_city_enc_table
;


-- card_mobile_city:
DROP TABLE IF EXISTS card_mobile_city_enc_table
;


CREATE TABLE card_mobile_city_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.enc     AS card_mobile_city 
   FROM tmp_rownum_table enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.card_mobile_city = enc_t.tmp_name
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( card_mobile_city_enc_table ) */ 
       lbenc_data.* 
       ,card_mobile_city 
   FROM lbenc_data 
 LEFT OUTER JOIN card_mobile_city_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS card_mobile_city_enc_table
;


-- card_cert_city:
DROP TABLE IF EXISTS card_cert_city_enc_table
;


CREATE TABLE card_cert_city_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.enc     AS card_cert_city 
   FROM tmp_rownum_table enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.card_cert_city = enc_t.tmp_name
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( card_cert_city_enc_table ) */ 
       lbenc_data.* 
       ,card_cert_city 
   FROM lbenc_data 
 LEFT OUTER JOIN card_cert_city_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS card_cert_city_enc_table
;


-- city:
DROP TABLE IF EXISTS city_enc_table
;


CREATE TABLE city_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.enc     AS city 
   FROM tmp_rownum_table enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.city = enc_t.tmp_name
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( city_enc_table ) */ 
       lbenc_data.* 
       ,city 
   FROM lbenc_data 
 LEFT OUTER JOIN city_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS city_enc_table
;


-- 构建证件号字段的编码表
DROP TABLE IF EXISTS tmp_table
;


CREATE TABLE tmp_table AS
  SELECT * 
   FROM (SELECT card_cert_no AS tmp_name 
           FROM data_all 
         UNION ALL 
         SELECT income_card_cert_no AS tmp_name 
           FROM data_all 
        )tmp
;


DROP TABLE IF EXISTS tmp_rownum_table
;


CREATE TABLE tmp_rownum_table AS
  SELECT tmp_name 
       ,ROW_NUMBER() OVER ( ORDER BY tmp_name ) AS enc 
   FROM tmp_table 
  WHERE tmp_name IS NOT NULL 
 GROUP BY tmp_name
;


DROP TABLE IF EXISTS tmp_table
;


-- card_cert_no:
DROP TABLE IF EXISTS card_cert_no_enc_table
;


CREATE TABLE card_cert_no_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.enc     AS card_cert_no 
   FROM tmp_rownum_table enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.card_cert_no = enc_t.tmp_name
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( card_cert_no_enc_table ) */ 
       lbenc_data.* 
       ,card_cert_no 
   FROM lbenc_data 
 LEFT OUTER JOIN card_cert_no_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS card_cert_no_enc_table
;


-- income_card_cert_no:
DROP TABLE IF EXISTS income_card_cert_no_enc_table
;


CREATE TABLE income_card_cert_no_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.enc     AS income_card_cert_no 
   FROM tmp_rownum_table enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.income_card_cert_no = enc_t.tmp_name
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( income_card_cert_no_enc_table ) */ 
       lbenc_data.* 
       ,income_card_cert_no 
   FROM lbenc_data 
 LEFT OUTER JOIN income_card_cert_no_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS income_card_cert_no_enc_table
;


-- 其余字段单独编码
-- client_ip:
DROP TABLE IF EXISTS client_ip_enc_table
;


CREATE TABLE client_ip_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.client_ip_enc AS client_ip 
   FROM (SELECT client_ip 
               ,ROW_NUMBER() OVER ( ORDER BY client_ip ) AS client_ip_enc 
           FROM data_all 
          WHERE client_ip IS NOT NULL 
         GROUP BY client_ip 
        )enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.client_ip = enc_t.client_ip
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( client_ip_enc_table ) */ 
       lbenc_data.* 
       ,client_ip 
   FROM lbenc_data 
 LEFT OUTER JOIN client_ip_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS client_ip_enc_table
;


-- network:
DROP TABLE IF EXISTS network_enc_table
;


CREATE TABLE network_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.network_enc AS network 
   FROM (SELECT network 
               ,ROW_NUMBER() OVER ( ORDER BY network ) AS network_enc 
           FROM data_all 
          WHERE network IS NOT NULL 
         GROUP BY network 
        )enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.network = enc_t.network
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( network_enc_table ) */ 
       lbenc_data.* 
       ,network 
   FROM lbenc_data 
 LEFT OUTER JOIN network_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS network_enc_table
;


-- device_sign:
DROP TABLE IF EXISTS device_sign_enc_table
;


CREATE TABLE device_sign_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.device_sign_enc AS device_sign 
   FROM (SELECT device_sign 
               ,ROW_NUMBER() OVER ( ORDER BY device_sign ) AS device_sign_enc 
           FROM data_all 
          WHERE device_sign IS NOT NULL 
         GROUP BY device_sign 
        )enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.device_sign = enc_t.device_sign
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( device_sign_enc_table ) */ 
       lbenc_data.* 
       ,device_sign 
   FROM lbenc_data 
 LEFT OUTER JOIN device_sign_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS device_sign_enc_table
;


-- info_1:
DROP TABLE IF EXISTS info_1_enc_table
;


CREATE TABLE info_1_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.info_1_enc AS info_1 
   FROM (SELECT info_1 
               ,ROW_NUMBER() OVER ( ORDER BY info_1 ) AS info_1_enc 
           FROM data_all 
          WHERE info_1 IS NOT NULL 
         GROUP BY info_1 
        )enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.info_1 = enc_t.info_1
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( info_1_enc_table ) */ 
       lbenc_data.* 
       ,info_1 
   FROM lbenc_data 
 LEFT OUTER JOIN info_1_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS info_1_enc_table
;


-- info_2:
DROP TABLE IF EXISTS info_2_enc_table
;


CREATE TABLE info_2_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.info_2_enc AS info_2 
   FROM (SELECT info_2 
               ,ROW_NUMBER() OVER ( ORDER BY info_2 ) AS info_2_enc 
           FROM data_all 
          WHERE info_2 IS NOT NULL 
         GROUP BY info_2 
        )enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.info_2 = enc_t.info_2
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( info_2_enc_table ) */ 
       lbenc_data.* 
       ,info_2 
   FROM lbenc_data 
 LEFT OUTER JOIN info_2_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS info_2_enc_table
;


-- is_one_people:
DROP TABLE IF EXISTS is_one_people_enc_table
;


CREATE TABLE is_one_people_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.is_one_people_enc AS is_one_people 
   FROM (SELECT is_one_people 
               ,ROW_NUMBER() OVER ( ORDER BY is_one_people ) AS is_one_people_enc 
           FROM data_all 
          WHERE is_one_people IS NOT NULL 
         GROUP BY is_one_people 
        )enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.is_one_people = enc_t.is_one_people
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( is_one_people_enc_table ) */ 
       lbenc_data.* 
       ,is_one_people 
   FROM lbenc_data 
 LEFT OUTER JOIN is_one_people_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS is_one_people_enc_table
;


-- mobile_oper_platform:
DROP TABLE IF EXISTS mobile_oper_platform_enc_table
;


CREATE TABLE mobile_oper_platform_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.mobile_oper_platform_enc AS mobile_oper_platform 
   FROM (SELECT mobile_oper_platform 
               ,ROW_NUMBER() OVER ( ORDER BY mobile_oper_platform ) AS mobile_oper_platform_enc 
           FROM data_all 
          WHERE mobile_oper_platform IS NOT NULL 
         GROUP BY mobile_oper_platform 
        )enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.mobile_oper_platform = enc_t.mobile_oper_platform
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( mobile_oper_platform_enc_table ) */ 
       lbenc_data.* 
       ,mobile_oper_platform 
   FROM lbenc_data 
 LEFT OUTER JOIN mobile_oper_platform_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS mobile_oper_platform_enc_table
;


-- operation_channel:
DROP TABLE IF EXISTS operation_channel_enc_table
;


CREATE TABLE operation_channel_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.operation_channel_enc AS operation_channel 
   FROM (SELECT operation_channel 
               ,ROW_NUMBER() OVER ( ORDER BY operation_channel ) AS operation_channel_enc 
           FROM data_all 
          WHERE operation_channel IS NOT NULL 
         GROUP BY operation_channel 
        )enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.operation_channel = enc_t.operation_channel
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( operation_channel_enc_table ) */ 
       lbenc_data.* 
       ,operation_channel 
   FROM lbenc_data 
 LEFT OUTER JOIN operation_channel_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS operation_channel_enc_table
;


-- pay_scene:
DROP TABLE IF EXISTS pay_scene_enc_table
;


CREATE TABLE pay_scene_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.pay_scene_enc AS pay_scene 
   FROM (SELECT pay_scene 
               ,ROW_NUMBER() OVER ( ORDER BY pay_scene ) AS pay_scene_enc 
           FROM data_all 
          WHERE pay_scene IS NOT NULL 
         GROUP BY pay_scene 
        )enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.pay_scene = enc_t.pay_scene
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( pay_scene_enc_table ) */ 
       lbenc_data.* 
       ,pay_scene 
   FROM lbenc_data 
 LEFT OUTER JOIN pay_scene_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS pay_scene_enc_table
;


-- income_card_no:
DROP TABLE IF EXISTS income_card_no_enc_table
;


CREATE TABLE income_card_no_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.income_card_no_enc AS income_card_no 
   FROM (SELECT income_card_no 
               ,ROW_NUMBER() OVER ( ORDER BY income_card_no ) AS income_card_no_enc 
           FROM data_all 
          WHERE income_card_no IS NOT NULL 
         GROUP BY income_card_no 
        )enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.income_card_no = enc_t.income_card_no
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( income_card_no_enc_table ) */ 
       lbenc_data.* 
       ,income_card_no 
   FROM lbenc_data 
 LEFT OUTER JOIN income_card_no_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS income_card_no_enc_table
;


-- income_card_mobile:
DROP TABLE IF EXISTS income_card_mobile_enc_table
;


CREATE TABLE income_card_mobile_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.income_card_mobile_enc AS income_card_mobile 
   FROM (SELECT income_card_mobile 
               ,ROW_NUMBER() OVER ( ORDER BY income_card_mobile ) AS income_card_mobile_enc 
           FROM data_all 
          WHERE income_card_mobile IS NOT NULL 
         GROUP BY income_card_mobile 
        )enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.income_card_mobile = enc_t.income_card_mobile
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( income_card_mobile_enc_table ) */ 
       lbenc_data.* 
       ,income_card_mobile 
   FROM lbenc_data 
 LEFT OUTER JOIN income_card_mobile_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS income_card_mobile_enc_table
;


-- income_card_bank_code:
DROP TABLE IF EXISTS income_card_bank_code_enc_table
;


CREATE TABLE income_card_bank_code_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.income_card_bank_code_enc AS income_card_bank_code 
   FROM (SELECT income_card_bank_code 
               ,ROW_NUMBER() OVER ( ORDER BY income_card_bank_code ) AS income_card_bank_code_enc 
           FROM data_all 
          WHERE income_card_bank_code IS NOT NULL 
         GROUP BY income_card_bank_code 
        )enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.income_card_bank_code = enc_t.income_card_bank_code
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( income_card_bank_code_enc_table ) */ 
       lbenc_data.* 
       ,income_card_bank_code 
   FROM lbenc_data 
 LEFT OUTER JOIN income_card_bank_code_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS income_card_bank_code_enc_table
;


-- is_peer_pay:
DROP TABLE IF EXISTS is_peer_pay_enc_table
;


CREATE TABLE is_peer_pay_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.is_peer_pay_enc AS is_peer_pay 
   FROM (SELECT is_peer_pay 
               ,ROW_NUMBER() OVER ( ORDER BY is_peer_pay ) AS is_peer_pay_enc 
           FROM data_all 
          WHERE is_peer_pay IS NOT NULL 
         GROUP BY is_peer_pay 
        )enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.is_peer_pay = enc_t.is_peer_pay
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( is_peer_pay_enc_table ) */ 
       lbenc_data.* 
       ,is_peer_pay 
   FROM lbenc_data 
 LEFT OUTER JOIN is_peer_pay_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS is_peer_pay_enc_table
;


-- ver:
DROP TABLE IF EXISTS ver_enc_table
;


CREATE TABLE ver_enc_table AS
  SELECT /*+ MAPJOIN ( enc_t ) */ 
       raw_t.event_id 
       ,enc_t.ver_enc AS ver 
   FROM (SELECT ver 
               ,ROW_NUMBER() OVER ( ORDER BY ver ) AS ver_enc 
           FROM data_all 
          WHERE ver IS NOT NULL 
         GROUP BY ver 
        )enc_t 
 RIGHT OUTER JOIN data_all raw_t 
     ON raw_t.ver = enc_t.ver
;


DROP TABLE IF EXISTS tmp_data
;


CREATE TABLE tmp_data AS
  SELECT /*+ MAPJOIN ( ver_enc_table ) */ 
       lbenc_data.* 
       ,ver 
   FROM lbenc_data 
 LEFT OUTER JOIN ver_enc_table enc_t 
     ON lbenc_data.event_id = enc_t.event_id
;


DROP TABLE IF EXISTS lbenc_data
;


CREATE TABLE lbenc_data AS
  SELECT * 
   FROM tmp_data
;


DROP TABLE IF EXISTS tmp_data
;


DROP TABLE IF EXISTS ver_enc_table
;


