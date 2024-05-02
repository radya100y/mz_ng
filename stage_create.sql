--> bonus_cur
drop table if exists integration.loyalty_bonus_cur;
create table integration.loyalty_bonus_cur
(
    before Nullable(String)
    , after Nullable(String)
    , source Nullable(String)
    , op Nullable(String)
    , ts_ms Nullable(UInt64)
    , ts_us Nullable(UInt64)
    , ts_ns Nullable(UInt64)
    , transaction Nullable(String)
) engine = Kafka()
    settings kafka_broker_list = '192.168.19.17:9014,192.168.19.17:9024,192.168.19.17:9034'
        , kafka_topic_list = 'postgres.loyalty.bonus_cur'
        , kafka_group_name = 'CH_group14'
        , kafka_format = 'JSONEachRow'
        , kafka_max_block_size = '512K'
        , input_format_skip_unknown_fields = 1
        , kafka_skip_broken_messages = 1;

drop table stage.loyalty_bonus_cur;
set  allow_experimental_object_type = 1;
create table stage.loyalty_bonus_cur
(
    key_id UInt64
    , before String
    , after String
    , source String
    , op String
    , ts_ms UInt64
    , ts_us UInt64
    , ts_ns UInt64
    , transaction String
    , replace_hash UInt64 materialized cityHash64(after)
) engine = ReplacingMergeTree(ts_ns)
order by (key_id, replace_hash);

drop table integration.mv_to_stage_loyalty_bonus_cur;
create materialized view integration.mv_to_stage_loyalty_bonus_cur to stage.loyalty_bonus_cur as
select
    cityHash64(_key) as key_id
    , assumeNotNull(before) as before
    , assumeNotNull(after) as after
    , assumeNotNull(source) as source
    , assumeNotNull(op) as op
    , assumeNotNull(ts_ms) as ts_ms
    , assumeNotNull(ts_us) as ts_us
    , assumeNotNull(ts_ns) as ts_ns
    , assumeNotNull(transaction) as transaction
from integration.loyalty_bonus_cur
settings stream_like_engine_allow_direct_select = 1;


--> bonus_wo_cur
drop table if exists integration.loyalty_bonus_wo_cur;
create table integration.loyalty_bonus_wo_cur
(
    before Nullable(String)
    , after Nullable(String)
    , source Nullable(String)
    , op Nullable(String)
    , ts_ms Nullable(UInt64)
    , ts_us Nullable(UInt64)
    , ts_ns Nullable(UInt64)
    , transaction Nullable(String)
) engine = Kafka()
    settings kafka_broker_list = '192.168.19.17:9014,192.168.19.17:9024,192.168.19.17:9034'
        , kafka_topic_list = 'postgres.loyalty.bonus_wo_cur'
        , kafka_group_name = 'CH_group14'
        , kafka_format = 'JSONEachRow'
        , kafka_max_block_size = '512K'
        , input_format_skip_unknown_fields = 1
        , kafka_skip_broken_messages = 1;

drop table stage.loyalty_bonus_wo_cur;
set  allow_experimental_object_type = 1;
create table stage.loyalty_bonus_wo_cur
(
    key_id UInt64
    , before String
    , after String
    , source String
    , op String
    , ts_ms UInt64
    , ts_us UInt64
    , ts_ns UInt64
    , transaction String
    , replace_hash UInt64 materialized cityHash64(after)
) engine = ReplacingMergeTree(ts_ns)
order by (key_id, replace_hash);

drop table integration.mv_to_stage_loyalty_bonus_wo_cur;
create materialized view integration.mv_to_stage_loyalty_bonus_wo_cur to stage.loyalty_bonus_wo_cur as
select
    cityHash64(_key) as key_id
    , assumeNotNull(before) as before
    , assumeNotNull(after) as after
    , assumeNotNull(source) as source
    , assumeNotNull(op) as op
    , assumeNotNull(ts_ms) as ts_ms
    , assumeNotNull(ts_us) as ts_us
    , assumeNotNull(ts_ns) as ts_ns
    , assumeNotNull(transaction) as transaction
from integration.loyalty_bonus_wo_cur
settings stream_like_engine_allow_direct_select = 1;


--> bonuscard
drop table if exists integration.crmdata_bonuscard;
create table integration.crmdata_bonuscard
(
    before Nullable(String)
    , after Nullable(String)
    , source Nullable(String)
    , op Nullable(String)
    , ts_ms Nullable(UInt64)
    , ts_us Nullable(UInt64)
    , ts_ns Nullable(UInt64)
    , transaction Nullable(String)
) engine = Kafka()
    settings kafka_broker_list = '192.168.19.17:9014,192.168.19.17:9024,192.168.19.17:9034'
        , kafka_topic_list = 'postgres.crmdata.bonuscard'
        , kafka_group_name = 'CH_group14'
        , kafka_format = 'JSONEachRow'
        , kafka_max_block_size = '512K'
        , input_format_skip_unknown_fields = 1
        , kafka_skip_broken_messages = 1;

drop table stage.crmdata_bonuscard;
set  allow_experimental_object_type = 1;
create table stage.crmdata_bonuscard
(
    key_id UInt64
    , before String
    , after String
    , source String
    , op String
    , ts_ms UInt64
    , ts_us UInt64
    , ts_ns UInt64
    , transaction String
    , replace_hash UInt64 materialized cityHash64(after)
) engine = ReplacingMergeTree(ts_ns)
order by (key_id, replace_hash);

drop table integration.mv_to_stage_crmdata_bonuscard;
create materialized view integration.mv_to_stage_crmdata_bonuscard to stage.crmdata_bonuscard as
select
    cityHash64(_key) as key_id
    , assumeNotNull(before) as before
    , assumeNotNull(after) as after
    , assumeNotNull(source) as source
    , assumeNotNull(op) as op
    , assumeNotNull(ts_ms) as ts_ms
    , assumeNotNull(ts_us) as ts_us
    , assumeNotNull(ts_ns) as ts_ns
    , assumeNotNull(transaction) as transaction
from integration.crmdata_bonuscard
settings stream_like_engine_allow_direct_select = 1;


--> contact
drop table if exists integration.crmdata_contact;
create table integration.crmdata_contact
(
    before Nullable(String)
    , after Nullable(String)
    , source Nullable(String)
    , op Nullable(String)
    , ts_ms Nullable(UInt64)
    , ts_us Nullable(UInt64)
    , ts_ns Nullable(UInt64)
    , transaction Nullable(String)
) engine = Kafka()
    settings kafka_broker_list = '192.168.19.17:9014,192.168.19.17:9024,192.168.19.17:9034'
        , kafka_topic_list = 'postgres.crmdata.contact'
        , kafka_group_name = 'CH_group14'
        , kafka_format = 'JSONEachRow'
        , kafka_max_block_size = '512K'
        , input_format_skip_unknown_fields = 1
        , kafka_skip_broken_messages = 1;

drop table stage.crmdata_contact;
set  allow_experimental_object_type = 1;
create table stage.crmdata_contact
(
    key_id UInt64
    , before String
    , after String
    , source String
    , op String
    , ts_ms UInt64
    , ts_us UInt64
    , ts_ns UInt64
    , transaction String
    , replace_hash UInt64 materialized cityHash64(after)
) engine = ReplacingMergeTree(ts_ns)
order by (key_id, replace_hash);

drop table integration.mv_to_stage_crmdata_contact;
create materialized view integration.mv_to_stage_crmdata_contact to stage.crmdata_contact as
select
    cityHash64(_key) as key_id
    , assumeNotNull(before) as before
    , assumeNotNull(after) as after
    , assumeNotNull(source) as source
    , assumeNotNull(op) as op
    , assumeNotNull(ts_ms) as ts_ms
    , assumeNotNull(ts_us) as ts_us
    , assumeNotNull(ts_ns) as ts_ns
    , assumeNotNull(transaction) as transaction
from integration.crmdata_contact
settings stream_like_engine_allow_direct_select = 1;


--> cheque_cur
drop table if exists integration.loyalty_cheque_cur;
create table integration.loyalty_cheque_cur
(
    before Nullable(String)
    , after Nullable(String)
    , source Nullable(String)
    , op Nullable(String)
    , ts_ms Nullable(UInt64)
    , ts_us Nullable(UInt64)
    , ts_ns Nullable(UInt64)
    , transaction Nullable(String)
) engine = Kafka()
    settings kafka_broker_list = '192.168.19.17:9014,192.168.19.17:9024,192.168.19.17:9034'
        , kafka_topic_list = 'postgres.loyalty.cheque_cur'
        , kafka_group_name = 'CH_group14'
        , kafka_format = 'JSONEachRow'
        , kafka_max_block_size = '512K'
        , input_format_skip_unknown_fields = 1
        , kafka_skip_broken_messages = 1;

drop table stage.loyalty_cheque_cur;
set  allow_experimental_object_type = 1;
create table stage.loyalty_cheque_cur
(
    key_id UInt64
    , before String
    , after String
    , source String
    , op String
    , ts_ms UInt64
    , ts_us UInt64
    , ts_ns UInt64
    , transaction String
    , replace_hash UInt64 materialized cityHash64(after)
) engine = ReplacingMergeTree(ts_ns)
order by (key_id, replace_hash);

drop table integration.mv_to_stage_loyalty_cheque_cur;
create materialized view integration.mv_to_stage_loyalty_cheque_cur to stage.loyalty_cheque_cur as
select
    cityHash64(_key) as key_id
    , assumeNotNull(before) as before
    , assumeNotNull(after) as after
    , assumeNotNull(source) as source
    , assumeNotNull(op) as op
    , assumeNotNull(ts_ms) as ts_ms
    , assumeNotNull(ts_us) as ts_us
    , assumeNotNull(ts_ns) as ts_ns
    , assumeNotNull(transaction) as transaction
from integration.loyalty_cheque_cur
settings stream_like_engine_allow_direct_select = 1;


--> chequeitem_cur
drop table if exists integration.loyalty_chequeitem_cur;
create table integration.loyalty_chequeitem_cur
(
    before Nullable(String)
    , after Nullable(String)
    , source Nullable(String)
    , op Nullable(String)
    , ts_ms Nullable(UInt64)
    , ts_us Nullable(UInt64)
    , ts_ns Nullable(UInt64)
    , transaction Nullable(String)
) engine = Kafka()
    settings kafka_broker_list = '192.168.19.17:9014,192.168.19.17:9024,192.168.19.17:9034'
        , kafka_topic_list = 'postgres.loyalty.chequeitem_cur'
        , kafka_group_name = 'CH_group14'
        , kafka_format = 'JSONEachRow'
        , kafka_max_block_size = '512K'
        , input_format_skip_unknown_fields = 1
        , kafka_skip_broken_messages = 1;

drop table stage.loyalty_chequeitem_cur;
set  allow_experimental_object_type = 1;
create table stage.loyalty_chequeitem_cur
(
    key_id UInt64
    , before String
    , after String
    , source String
    , op String
    , ts_ms UInt64
    , ts_us UInt64
    , ts_ns UInt64
    , transaction String
    , replace_hash UInt64 materialized cityHash64(after)
) engine = ReplacingMergeTree(ts_ns)
order by (key_id, replace_hash);

drop table integration.mv_to_stage_loyalty_chequeitem_cur;
create materialized view integration.mv_to_stage_loyalty_chequeitem_cur to stage.loyalty_chequeitem_cur as
select
    cityHash64(_key) as key_id
    , assumeNotNull(before) as before
    , assumeNotNull(after) as after
    , assumeNotNull(source) as source
    , assumeNotNull(op) as op
    , assumeNotNull(ts_ms) as ts_ms
    , assumeNotNull(ts_us) as ts_us
    , assumeNotNull(ts_ns) as ts_ns
    , assumeNotNull(transaction) as transaction
from integration.loyalty_chequeitem_cur
settings stream_like_engine_allow_direct_select = 1;


select * from stage.loyalty_chequeitem_cur;

select
    toInt64OrZero(JSON_VALUE((op = 'd' ? before : after as ba), '$.bonus_id')) as bonus_id
    , parseDateTimeBestEffortOrZero(JSON_VALUE(ba, '$.created_on'), 'UTC') as created_on
    , toDecimal32OrZero(JSON_VALUE(ba, '$.value'), 2) as value
    , toDecimal32OrZero(JSON_VALUE(ba, '$.discount'), 2) as discount
    , parseDateTimeBestEffortOrZero(JSON_VALUE(ba, '$.start_date'), 'UTC') as start_date
    , parseDateTimeBestEffortOrZero(JSON_VALUE(ba, '$.finish_date'), 'UTC') as finish_date
    , toDecimal32OrZero(JSON_VALUE(ba, '$.remainder'), 2) as remainder
    , toInt64OrZero(JSON_VALUE(ba, '$.cheque_id')) as cheque_id
    , toInt64OrZero(JSON_VALUE(ba, '$.cheque_item_id')) as cheque_item_id
    , toInt32OrZero(JSON_VALUE(ba, '$.parent_type_id')) as parent_type_id
    , JSON_VALUE(ba, '$.operation_type_id') as operation_type_id
    , parseDateTimeBestEffortOrZero(JSON_VALUE(ba, '$.processed_date'), 'UTC') as processed_date
    , parseDateTimeBestEffortOrZero(JSON_VALUE(ba, '$.disposed_date'), 'UTC') as disposed_date
    , parseDateTimeBestEffortOrZero(JSON_VALUE(ba, '$.old_finish_date'), 'UTC') as old_finish_date
    , toInt64OrZero(JSON_VALUE(ba, '$.source_bonus_id')) as source_bonus_id
    , toInt32OrZero(JSON_VALUE(ba, '$.partition_chequeitem_num')) as partition_chequeitem_num
    , toInt8OrZero(JSON_VALUE(ba, '$.partner_balance')) as partner_balance
    , toInt8OrZero(JSON_VALUE(ba, '$.isstatus')) as isstatus
    , toInt64OrZero(JSON_VALUE(ba, '$.order_id')) as order_id
    , toInt64OrZero(JSON_VALUE(ba, '$.orderitem_id')) as orderitem_id
    , toInt32OrZero(JSON_VALUE(ba, '$.organization_id')) as organization_id
    , toInt32OrZero(JSON_VALUE(ba, '$.campaign_id')) as campaign_id
    , toInt32OrZero(JSON_VALUE(ba, '$.rule_id')) as rule_id
    , toInt32OrZero(JSON_VALUE(ba, '$.card_id')) as card_id
    , toInt32OrZero(JSON_VALUE(ba, '$.created_by')) as created_by
    , toInt32OrZero(JSON_VALUE(ba, '$.finished_by')) as finished_by
    , toInt64OrZero(JSON_VALUE(ba, '$.parent_id')) as parent_id
    , op as sys_change_operation
    , ts_ms as last_version
    , parseDateTimeBestEffortOrZero(toString(ts_ms), 'UTC') as dt_load
from stage.loyalty_bonus_cur
order by ts_ms;
