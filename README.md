# ATEC蚂蚁开发者大赛-支付风险识别-Rank8
## 赛题描述
赛题的目的是根据历史交易数据识别当前交易是否为欺诈交易。举办方给出由一段时间内有正负标签样本的支付行为样本和没有标签的支付行为样本组成的训练数据集和一段时间后的某个时间范围内的支付行为样本构成的测试数据集，希望选手们通过机器学习算法和对无标签数据的挖掘在训练集上训练出性能稳定时效性好的模型，能够在测试集上对交易的风险进行精准判断。

### 复赛数据集字段描述
特征名 | 特征描述
--- | ---
event_id|事件id
user_id|虚拟用户ID
gmt_occur|事件发生时间
client_ip|用户IP
network|网络类型
device_sign|设备ID
info1|信息1
info2|信息2
ip_prov|IP省
ip_city|IP市
cert_prov|证件省
cert_city|证件市
card_bin_prov|支付卡bin省
card_bin_city|支付卡bin市
card_mobile_prov|支付账号省
card_mobile_city|支付账号市
card_cert_prov|支付卡省
card_cert_city|支付卡市
is_one_people|主次双方证件是否一致
mobile_oper_platform|手机操作平台
operation_channel|支付方式
pay_scene|支付场景
amt|金额
card_cert_no|虚拟用户证件号
opposing_id|对方虚拟用户ID
income_card_no|虚拟用户的收款银行卡号
income_card_cert_no|虚拟收款用户的证件号
income_card_mobile|虚拟收款用户的手机号
income_card_bank_code|收入账号银行代码
province|收入账号归属省份
city|虚拟收款用户归属城市
is_peer_pay|是否代付
version|版本号
is_fraud|预测标签


## 数据编码
代码：cbc/preprocess/label_encoder.sql

功能：将 string 型特征编码为 int 型

同类里统一编码的字段：

**用户ID字段：**

user_id, opposing_id

**省份字段：**

ip_prov, cert_prov, card_bin_prov, card_mobile_prov, card_cert_prov,   province

**城市字段：**

ip_city, cert_city, card_bin_city, card_mobile_city, card_cert_city, city

**证件号字段：**

card_cert_no, income_card_cert_no

**其余字段单独编码**



## 特征工程

### 提取时间特征
代码：cbc/preprocess/time_field_process.sql

功能：对 gmt_occur 进行分割处理，分离出来 ocu_date 和 ocu_hr，另外增加了 year, month, day 等特征。

### 付款方的众数特征 （历史时间）
代码：cbc/feature_engineering/user_id_is_mode_feature_cbc.sql

表名：user_id_is_mode_feature_cbc

特征名 | 特征描述
--- | ---
ip_is_most|当前 client_ip 是否为 user_id 的众数
network_is_most|当前 network 是否为 user_id 的众数
device_sign_is_most|当前 device_sign 是否为 user_id 的众数
ip_prov_is_most|当前 ip_prov 是否为 user_id 的众数
ip_city_is_most|当前 ip_city 是否为 user_id 的众数
user_id_cert_prov_is_mode|当前 cert_prov 是否为 user_id 的众数
user_id_cert_city_is_mode|当前 cert_city 是否为 user_id 的众数
user_id_card_mobile_prov_is_mode|当前 card_mobile_prov 是否为 user_id 的众数
user_id_card_mobile_city_is_mode|当前 card_mobile_city 是否为 user_id 的众数
user_id_card_cert_prov_is_mode|当前 card_cert_prov 是否为 user_id 的众数
user_id_card_cert_city_is_mode|当前 card_cert_city 是否为 user_id 的众数
mobile_oper_platform_is_most|当前 mobile_oper_platform 是否为 user_id 的众数
operation_channel_is_most|当前 operation_channel 是否为 user_id 的众数
pay_scene_is_most|当前 pay_scene 是否为 user_id 的众数
user_id_card_cert_no_is_mode|当前 card_cert_no 是否为 user_id 的众数
user_id_opposing_id_is_mode|当前 opposing_id 是否为 user_id 的众数
user_id_ver_is_mode|当前 ver 是否为 user_id 的众数

### user_id在各字段下不同值的个数特征（历史时间)
代码：cbc/feature_engineering/user_id_dist_cnt_feature.sql

表名：user_id_dist_cnt_feature_cbc

特征名 | 特征描述
--- | ---
user_id_client_ip_dist_cnt|user_id 不同的 ip 数目
user_id_network_dist_cnt| user_id 不同的 network 数目
user_id_device_sign_dist_cnt| user_id 不同的 device_sign 数目
user_id_ip_prov_dist_cnt| user_id 不同的ip省数目
user_id_ip_city_dist_cnt| user_id 不同的ip市数目
user_id_cert_prov_dist_cnt| user_id 不同的证件省数目
user_id_cert_city_dist_cnt|user_id 不同的证件市数目
user_id_card_mobile_prov_dist_cnt|user_id 不同的手机账号省数目
user_id_card_mobile_city_dist_cnt|user_id 不同的手机账户市数目
user_id_card_cert_prov_dist_cnt|user_id 不同的银行卡省数目
user_id_card_cert_city_dist_cnt|user_id 不同的银行卡市数目
user_id_mobile_oper_platform_dist_cnt|user_id 不同的操作平台数目
user_id_operation_channel_dist_cnt|user_id 不同的支付方式数目
user_id_pay_scene_dist_cnt|user_id 不同的支付场景数目
user_id_card_cert_no_dist_cnt|user_id 不同的证件号数目
user_id_opposing_id_dist_cnt|user_id 不同的 opposing_id 数目
user_id_ver_dist_cnt|user_id 不同的版本数目


### opposing_id 在各字段下不同值的个数特征（历史时间)
代码：cbc/feature_engineering/opposing_id_dist_cnt_feature.sql

表名：opposing_id_dist_cnt_feature_cbc

特征名 | 特征描述
--- | ---
opposing_id_user_id_dist_cnt|opposing_id 不同的 user_id 数目
opposing_id_client_ip_dist_cnt|opposing_id 不同的 ip 数目
opposing_id_network_dist_cnt| opposing_id 不同的 network 数目
opposing_id_device_sign_dist_cnt| opposing_id 不同的 device_sign 数目
opposing_id_ip_prov_dist_cnt| opposing_id 不同的ip省数目
opposing_id_ip_city_dist_cnt| opposing_id 不同的ip市数目
opposing_id_cert_prov_dist_cnt| opposing_id 不同的证件省数目
opposing_id_cert_city_dist_cnt|opposing_id 不同的证件市数目
opposing_id_card_mobile_prov_dist_cnt|opposing_id 不同的手机账号省数目
opposing_id_card_mobile_city_dist_cnt|opposing_id 不同的手机账户市数目
opposing_id_card_cert_prov_dist_cnt|opposing_id 不同的银行卡省数目
opposing_id_card_cert_city_dist_cnt|opposing_id 不同的银行卡市数目
opposing_id_mobile_oper_platform_dist_cnt|opposing_id 不同的操作平台数目
opposing_id_operation_channel_dist_cnt|opposing_id 不同的支付方式数目
opposing_id_pay_scene_dist_cnt|opposing_id 不同的支付场景数目
opposing_id_card_cert_no_dist_cnt|opposing_id 不同的证件号数目
opposing_id_ver_dist_cnt|opposing_id 不同的版本数目


### client_ip 在各字段下不同值的个数特征（历史时间)
代码：cbc/feature_engineering/client_ip_dist_cnt_feature.sql

表名：client_ip_dist_cnt_feature_cbc

特征名 | 特征描述
--- | ---
client_ip_user_id_dist_cnt|client_ip 不同的 user_id 数目
client_ip_network_dist_cnt| client_ip 不同的 network 数目
client_ip_device_sign_dist_cnt| client_ip 不同的 device_sign 数目
client_ip_cert_prov_dist_cnt| client_ip 不同的证件省数目
client_ip_cert_city_dist_cnt|client_ip 不同的证件市数目
client_ip_card_mobile_prov_dist_cnt|client_ip 不同的手机账号省数目
client_ip_card_mobile_city_dist_cnt|client_ip 不同的手机账户市数目
client_ip_card_cert_prov_dist_cnt|client_ip 不同的银行卡省数目
client_ip_card_cert_city_dist_cnt|client_ip 不同的银行卡市数目
client_ip_mobile_oper_platform_dist_cnt|client_ip 不同的操作平台数目
client_ip_operation_channel_dist_cnt|client_ip 不同的支付方式数目
client_ip_pay_scene_dist_cnt|client_ip 不同的支付场景数目
client_ip_card_cert_no_dist_cnt|client_ip 不同的证件号数目
client_ip_opposing_id_dist_cnt|client_ip 不同的 opposing_id 数目
client_ip_ver_dist_cnt|client_ip 不同的版本数目


### 付款方当前小时的交易金额特征
代码：cbc/feature_engineering/payer_amt_1hr_feature.sql

表名： payer_amt_1hr_feature

特征名 | 特征描述
--- | ---
payer_amt_sum_1hr|付款方一小时内消费总额
payer_amt_avg_1hr|付款方一小时内平均金额
payer_amt_max_1hr|付款方一小时内最大金额
payer_amt_min_1hr|付款方一小时内最小金额
amt_subtract_avg_1hr|付款方当前金额减一小时平均
amt_subtract_max_1hr|付款方当前金额减一小时最大
amt_subtract_min_1hr|付款方当前金额减一小时最小

### 收款方的历史交易金额特征
代码：cbc/feature_engineering/payee_amt_feature_cbc.sql

表名：payee_amt_feature_cbc

特征名 | 特征描述
--- | ---
payee_amt_avg|收款方历史时间收款平均值
payee_amt_max|收款方历史时间收款最大值
payee_amt_min|收款方历史时间收款最小值
payee_amt_minus_avg|收款方金额减平均值
payee_amt_minus_max|收款方金额减最大值
payee_amt_minus_min|收款方金额减最小值

### 频次特征

代码：qyk/

表名：atec_all_data_feature_10

特征名 | 特征描述
--- | ---
freq_deal_sum | 过去所有历史交易的频次特征
freq_deal_1day | 当前一天截止当笔交易的频次特征
freq_deal_1hr | 当前一小时交易的频次特征

### 补充频次特征
代码：cbc/feature_engineering/feature_cnt_cbc.sql

表名为 extra_cnt_featurce_cbc

特征名 | 特征描述
--- | ---
payee_1hr_get_cnt|收款方一小时内收款次数 
ip_1hr_cnt|ip一小时内出现的次数
ip_appear_cnt |  此次交易的ip在之前出现的次数


### 二补频次特征
代码：cbc/feature_engineering/cnt_feature_pack_2_cbc.sql

表名：cnt_feature_pack_2_cbc

特征名 | 特征描述
--- | ---
payee_get_cnt|收款方的历史收款总次数
ip_1day_cnt | ip当天内出现的次数（此交易前）
payee_1day_get_cnt| 收款方当天内收款次数（此交易前）
peer_pay_cnt|历史时间里代付的次数
peer_pay_1day_cnt|当天里代付的次数（此交易前）
peer_pay_1hr_cnt|一小时内代付的次数
device_sign_1hr_cnt|设备ID一小时内出现的次数
device_sign_1day_cnt|设备ID当天内出现的次数（此交易前）
device_sign_cnt|设备ID历史出现的次数
cert_no_1hr_cnt|付款方证件号一小时内出现的次数
cert_no_1day_cnt|付款方证件号当天内出现的次数（此交易前）
cert_no_cnt|付款方证件号历史出现的次数

### 前一天的频次特征
代码：cbc/feature_engineering/lastday_cnt_feature.sql

表名：lastday_cnt_feature_cbc

特征名 | 特征描述
--|--
user_id_cnt_lastday|前一天 user_id 出现的频次
opposing_id_cnt_lastday|前一天 opposing_id 出现的频次
client_ip_cnt_lastday|前一天 client_ip 出现的频次
device_sign_cnt_lastday|前一天 device_sign 出现的频次
card_cert_no_cnt_lastday|前一天 card_cert_no 出现的频次

### 历史曾出现的最近一天里的频次特征
代码：qyk/

表名：atec_all_data_feature_61


特征名 | 特征描述
--|--
lag1_freq_deal_sum1day|交易频次特征（历史曾出现的最近一天）
opposing_id_num_lag1day|opposing_id出现的频次（历史曾出现的最近一天）
client_ip_num_lag1day|client_ip出现的频次（历史曾出现的最近一天）
device_sign_num_lag1day|device_sign出现的频次（历史曾出现的最近一天）
card_cert_no_num_lag1day|card_cert_no出现的频次（历史曾出现的最近一天）


### 历史所有天里的天平均频次
代码：qyk/

表名：atec_all_data_feature_82

特征名 | 特征描述
--- | ---
opposing_id_lag1day_avg_real| opposing_id前一天及之前平均频次
client_ip_lag1day_avg_real|client_ip前一天及之前平均频次
device_sign_lag1day_avg_real|device_sign前一天及之前平均频次
user_id_lag1day_avg_real|付款方前一天及之前平均频次

### 历史所有小时里的小时平均频次
代码：qyk/

表名：atec_all_data_feature_92

特征名 | 特征描述
--- | ---
user_id_freq_deal_1hr_avg_real| user_id小时及之前平均小时频次
opposing_id_freq_deal_1hr_avg_real|opposing_id小时及之前平均小时频次
client_ip_freq_deal_1hr_avg_real|client_ip小时及之前平均小时频次
device_sign_freq_deal_1hr_avg_real|device_sign小时及之前平均小时频次


### 历史所有相同时钟下的时钟平均频次
代码：cbc/feature_engineering/avg_clock_cnt_feature.sql

表名：avg_cnt_clock_cbc

特征名 | 特征描述
--- | ---
uid_cnt_clock_avg|付款方历史所有相同时钟下的平均频次
opid_cnt_clock_avg|收款方历史所有相同时钟下的平均频次
ip_cnt_clock_avg|ip 历史所有相同时钟下的平均频次
device_cnt_clock_avg|设备 ID 历史所有相同时钟下的平均频次
uid_avg_cnt_clock_subtract|付款方历史所有相同时钟下的平均频次减当前小时频次
opid_avg_cnt_clock_subtract|收款方历史所有相同时钟下的平均频次减当前小时频次
ip_avg_cnt_clock_subtract|ip 历史所有相同时钟下的平均频次减当前小时频次
device_avg_cnt_clock_subtract|设备id 历史所有相同时钟下的平均频次减当前小时频次



### 历史所有相同星期下的星期平均频次
代码：qyk/

表名：atec_all_data_feature_110


特征名 | 特征描述
--- | ---
freq_deal_weekday_avg|付款方历史所有相同星期下的平均频次
freq_opposing_id_weekday_avg|收款方历史所有相同星期下的平均频次
freq_client_ip_weekday_avg|client_ip历史所有相同星期下的平均频次
freq_device_sign_weekday_avg|device_sign历史所有相同星期下的平均频次

### 上一天及之前曾出现的天里的平均频次特征
代码：qyk/

表名：atec_all_data_feature_42

特征名 | 特征描述
--- | ---
opposing_id_num_lag1day_avg|上一天及之前的 opposing_id 天频次平均
client_ip_num_lag1day_avg|上一天及之前的 client_ip 天频次平均
device_sign_num_lag1day_avg|上一天及之前的 device_sign 天频次平均

### 历史曾出现的小时里的平均频次特征
代码：qyk/

表名：atec_all_data_feature_52
特征名 | 特征描述
--- | ---
opposing_id_num_1hr_avg|本小时及之前 opposing_id 出现的小时频次平均
client_ip_num_1hr_avg|本小时及之前的 client_ip 出现的小时频次平均
device_sign_num_1hr_avg|本小时及之前的 device_sign 出现的小时频次平均



### 上一次交易特征
代码：qyk/train1_5train_data_process.sql,  qyk/test1_5test_data_process.sql

特征名 | 特征含义及描述
---|---
prev1amt | 上一次交易消费金额（训练集）
prev1client_ip | 上一次交易用户IP
prev1network | 上一次交易网络类型
prev1device_sign | 上一次设备ID
prev1mobile_plat | 上一次交易手机平台
prev1oper_channel | 上一次交易支付方式
prev1pay_scene | 上一次交易支付场景
is_ip_the_same | 本次交易和上次交易IP是否相同
is_network_same | 本次和上次网络类型是否相同
is_device_sign_same | 本次和上次设备ID是否相同
is_mobile_plat_same | 手机平台是否相同
is_oper_channel_same | 支付方式是否项目
is_pay_scene_same | 支付场景是否相同

### 上一天金额特征
代码：qyk/

表名：atec_train_data_feature_4,  atec_test_data_feature_4

特征名 | 特征含义及描述
---|---
lastday_amt_sum | 上一天天消费总额
lastday_amt_avg | 上一天交易金额平均
lastday_amt_max | lastday交易金额最大
lastday_amt_min | lastday交易金额最小

### 过去三天金额的特征
代码：qyk/

表名：atec_train_data_feature_5, atec_test_data_feature_5

特征名 | 特征含义及描述
---|---
lastThreeday_amt_sum | lastThreeday交易金额之和
lastThreeday_amt_avg | lastThreeday交易金额平均
lastThreeday_amt_max | lastThreeday交易金额最大
lastThreeday_amt_min | lastThreeday交易金额最小

### 所有历史金额的特征
代码：qyk/

表名：atec_all_data_feature_1
特征名 | 特征含义及描述
---|---
amt_avg | 历史交易金额平均（此交易前）
amt_max | 历史交易金额最大（此交易前）
amt_min | 历史交易金额最小（此交易前）

### 历史交易金额和当次消费相减的特征
代码：qyk/

表名：atec_all_data_feature_6

特征名 | 特征含义及描述
---|---
amt_subtract_avg | 当前金额减去历史交易平均
amt_subtract_max | 当前金额减去历史金额最大
amt_substact_min | 当前金额减去历史金额最小

### 支付账户的收款特征

代码： qyk/

表名：atec_all_data_feature_5

特征名 | 特征含义及描述
---|---
receive_num | 该支付账户本次交易前收款次数
receive_amt_avg | 该支付账户本次交易前收款平均额度
receive_amt_max | 该支付账户本次交易前收款最大额度



### 基于星期属性的特征
代码：qyk/

表名：atec_all_data_feature_11

特征名 | 特征描述
--- | ---
amt_deal_weekday_avg| 截止当前交易周中的第几天累计历史交易平均额度
freq_deal_weekday|截止当前交易周中的第几天累计历史交易频次



### 峰度偏度及其他一些统计量

#### 交易金额相关统计量
代码：qyk/

表名：atec_all_data_feature_14

特征名 | 特征描述
--- | ---
stdv_amt | 历史交易金额标准差
skewness_amt | 历史交易金额的偏度
kurtosis_amt | 历史交易金额的峰度

#### 一小时频次的偏度和峰度

代码：qyk/

表名：atec_all_data_feature_24

特征名 | 特征描述
--- | ---
stdv_freq_deal_1hr | 这一小时及之前的小时交易频次标准差
avg_freq_deal_1hr | 这一小时及之前的小时交易频次均值
skewness_freq_1hr | 这一小时及之前的小时交易频次偏度
kurtosis_freq_1hr | 这一小时及之前的小时交易频次峰度

#### 一天频次的偏度和峰度
代码：qyk/

表名：atec_all_data_feature_22

特征名|特征描述
---|---
stdv_lag1_freq_deal_sum1day|上一天之前的交易频次平方差
avg_lag1_freq_deal_sum1day | 上一天之前的交易频次平均
skewness_freq_sum1day | 上一天之前的交易频次偏度
kurtosis_freq_sum1day |  上一天之前的交易频次峰度


### user_id 在各字段下不同值的个数特征（一小时内）
代码：cbc/feature_engineering/dist_cnt_feature/dist_cnt_1hr_feature.sql


表名：uid_dist_cnt_1hr

特征名 | 特征描述
--- | ---
uid_ip_dist_cnt_1hr|付款方不同的ip数目
uid_network_dist_cnt_1hr|付款方不同的网络类型数目
uid_device_sign_dist_cnt_1hr|付款方不同的设备id数目
uid_ip_prov_dist_cnt_1hr|付款方不同的ip省数目
uid_card_mobile_prov_dist_cnt_1hr|付款方不同的支付账户省数目
uid_mobile_oper_platform_dist_cnt_1hr|付款方不同的操作平台数目
uid_operation_channel_dist_cnt_1hr|付款方不同的支付方式数目
uid_pay_scene_dist_cnt_1hr|付款方不同的支付场景数目
uid_card_cert_no_dist_cnt_1hr|付款方不同的付款人证件号数目
uid_ver_dist_cnt_1hr|付款方不同的版本数目
uid_oppoid_dist_cnt_1hr|付款方不同的收款人数目


### opposing_id 在各字段下不同值的个数特征（一小时内）
代码：cbc/feature_engineering/dist_cnt_feature/dist_cnt_1hr_feature.sql

表名：oppoid_dist_cnt_1hr


特征名 | 特征描述
--- | ---
oppoid_uid_dist_cnt_1hr|收款方不同的付款人数目
oppoid_ip_dist_cnt_1hr|收款方不同的ip数目
oppoid_network_dist_cnt_1hr|收款方不同的网络类型数目
oppoid_device_sign_dist_cnt_1hr|收款方不同的设备id数目
oppoid_ip_prov_dist_cnt_1hr|收款方不同的ip省数目
oppoid_card_mobile_prov_dist_cnt_1hr|收款方不同的支付账户省数目
oppoid_mobile_oper_platform_dist_cnt_1hr|收款方不同的操作平台数目
oppoid_operation_channel_dist_cnt_1hr|收款方不同的支付方式数目
oppoid_pay_scene_dist_cnt_1hr|收款方不同的支付场景数目
oppoid_card_cert_no_dist_cnt_1hr|收款方不同的付款人证件号数目
oppoid_ver_dist_cnt_1hr|收款方不同的版本数目


### client_ip 在各字段下不同值的个数特征（一小时内）
代码：cbc/feature_engineering/dist_cnt_feature/dist_cnt_1hr_feature.sql

表名：ip_dist_cnt_1hr

特征名 | 特征描述
--- | ---
ip_uid_dist_cnt_1hr|当前ip不同的付款人数目
ip_network_dist_cnt_1hr|当前ip不同的网络类型数目
ip_device_dist_cnt_1hr|当前ip不同的设备id数目
ip_cert_prov_dist_cnt_1hr|当前ip不同的证件省数目
ip_mobile_prov_dist_cnt_1hr|当前ip不同的手机省数目
ip_plat_dist_cnt_1hr|当前ip不同的操作平台数目
ip_channel_dist_cnt_1hr|当前ip不同的支付方式数目
ip_pay_scene_dist_cnt_1hr|当前ip不同的支付场景数目
ip_cert_no_dist_cnt_1hr|当前ip不同的证件号数目
ip_opposing_id_dist_cnt_1hr|当前ip不同的收款人数目
ip_ver_dist_cnt_1hr|当前ip不用的版本号数目

### 时间循环特征
代码：qyk/

表名： atec_all_data_feature_25

特征名 | 特征描述
--- | ---
hour_circu|小时循环特征
weekday_circu|星期循环特征

### 过去几次消费的特征
代码：qyk/

表名：atec_all_data_feature_26

特征名 | 特征描述
--- | ---
prev5_amt_max|过去5笔交易金额的最大值
prev5_amt_avg|过去5笔交易金额的平均值
prev7_amt_max|过去7笔交易金额的最大值
prev7_amt_avg|过去7笔交易金额的平均值
prev10_amt_max|过去10笔交易金额的最大值
prev10_amt_avg|过去10笔交易金额的平均值

### 消费时间间隔特征
代码：qyk/

表名：atec_all_data_feature_28

特征名 | 特征描述
--- | ---
time_interval1|本次交易和上次交易的时间间隔


### 消费金额连续相同次数
代码：qyk/

表名：atec_all_data_feature_53
特征名 | 特征描述
--- | ---
same_amt_num|消费金额连续相同次数


### 地区相等特征
代码: cbc/feature_engineering/added_feature_cbc_0803.sql

表名：feature_0803_cbc

特征名 | 特征描述
--- | ---
uid_opid_cnt_1hr| user_id 与 opposing_id 一小时内的交易次数
uid_opid_cnt_1day_pre|付款方与收款方上一天里的交易次数
uid_opid_cnt_all| 付款方与收款方历史交易总次数
ip_prov_equal_cert_prov|ip省是否等于证件省
ip_prov_equal_mobile_prov|ip省是否等于手机账号省
ip_prov_equal_card_prov|ip省是否等于银行卡省
ip_city_equal_cert_city|ip市是否等于证件市
ip_city_equal_mobile_city|ip市是否等于手机账号市
ip_city_equal_card_city | ip市是否等于银行卡市

### 对应字段在user_id下的比例特征
代码：qyk/

表名：atec_all_data_feature_145

特征名 | 特征描述
--- | ---
ip_ratio|当前client_ip使用比例
network_ratio|当前network使用比例
device_sign_ratio|当前 device_sign 使用比例
ip_prov_ratio|当前 ip_prov 使用比例
ip_city_ratio|当前 ip_city 使用比例
card_mobile_prov_ratio|当前 card_mobile_prov 使用比例
mobile_oper_platform_ratio|当前 mobile_oper_platform 使用比例
operation_channel_ratio|当前 operation_channel 使用比例
pay_scene_ratio|当前 pay_scene 使用比例
card_cert_no_ratio|当前 card_cert_no 使用比例
ver_ratio|当前 version 使用比例
opposing_id_ratio| 当前 opposing_id 比例

### 节假日特征
代码：qyk/

表名：atec_all_data_feature_201

特征名 | 特征描述
--- | ---
month_end_day|月末最后两天
is_holiday|是否为节假日
holiday_series|节假日序列特征