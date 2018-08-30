-- 补充地区是否相等的特征
DROP TABLE IF EXISTS added_area_equal_feature_cbc
;


CREATE TABLE added_area_equal_feature_cbc AS
  SELECT event_id 
       ,CAST ( cert_prov = card_mobile_prov AS INT )      AS cert_prov_equal_mobile_prov -- 证件省是否等于手机账号省
       ,CAST ( cert_prov = card_cert_prov AS INT )        AS cert_prov_equal_card_prov   --证件省是否等于银行卡省
       ,CAST ( card_mobile_prov = card_cert_prov AS INT ) AS mobile_prov_equal_card_prov --手机账户省是否等于银行卡省
       ,CAST ( cert_city = card_mobile_city AS INT )      AS cert_city_equal_mobile_city --证件市是否等于手机账户市
       ,CAST ( cert_city = card_cert_city AS INT )        AS cert_city_equal_card_city   --证件市是否等于银行卡市
       ,CAST ( card_mobile_city = card_cert_city AS INT ) AS mobile_city_equal_card_city --手机账户市是否等于银行卡市
   FROM atec_all_data_series