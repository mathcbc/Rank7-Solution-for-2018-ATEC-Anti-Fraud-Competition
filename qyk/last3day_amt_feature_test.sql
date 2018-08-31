--过去3天历史交易金额特征
drop table if EXISTS atec_test_data_feature_5;
create table atec_test_data_feature_5
as select 
event_id,
sum(amt) as lastThreeday_amt_sum, --lastThreeday交易金额之和
avg(amt) as lastThreeday_amt_avg, --lastThreeday交易金额平均
max(amt) as lastThreeday_amt_max, --lastThreeday交易金额最大
min(amt) as lastThreeday_amt_min -- lastThreeday交易金额最小
from (select t2.amt,t1.event_id from atec_test_series t1 left outer join atec_test_series t2
on t1.user_id = t2.user_id 
where ( (t1.ocu_date >=t2.ocu_date) and (t1.ocu_date <= dateadd(t2.ocu_date,3,'day') ) ) ) tmp
group by event_id;