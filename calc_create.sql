create database if not exists calc;

-- drop table if exists calc.ci;
create table calc.ci
(
    ci_instance_hash UInt64
    , key_instance_source_hash UInt64
    , instance_id LowCardinality(String)
    , source_id LowCardinality(String)
    , article_hash UInt64
    , price Decimal32(2)
    , quantity Decimal32(3)
    , summdisc Decimal32(2)
    , summ Decimal32(2)
    , discount Decimal32(2)
    , is_del UInt8
    , ch_id String
    , ci_id String
    , article_id String
    , ts_ms UInt64
    , dt_stream DateTime materialized parseDateTimeBestEffortOrZero(toString(ts_ms), 'UTC')
) engine = ReplacingMergeTree(ts_ms)
partition by toYYYYMM(dt_stream)
order by (ci_instance_hash, key_instance_source_hash)
primary key ci_instance_hash;





-- drop table if exists integration.mv_to_calc_ci_from_loyalty_chequeitem_cur;
create materialized view integration.mv_to_calc_ci_from_loyalty_chequeitem_cur to calc.ci as
select
    cityHash64(ci_id, instance_id) as ci_instance_hash
    , cityHash64(ci_id, instance_id, source_id) as key_source_instance_hash
    , JSON_VALUE(source, '$.name') as instance_id
    , concat(lower(JSON_VALUE(source, '$.schema')), '.', lower(JSON_VALUE(source, '$.table'))) as source_id
    , cityHash64(article_id, instance_id) as article_hash
    , toDecimal32OrZero(JSON_VALUE(ba, '$.price'), 2) as price
    , toDecimal32OrZero(JSON_VALUE(ba, '$.quantity'), 2) as quantity
    , toDecimal32OrZero(JSON_VALUE(ba, '$.summdisc'), 2) as summdisc
    , toDecimal32OrZero(JSON_VALUE(ba, '$.summ'), 2) as summ
    , toDecimal32OrZero(JSON_VALUE(ba, '$.discount'), 2) as discount
    , op = 'd' as is_del
    , toInt64OrZero(JSON_VALUE(ba, '$.cheque_id')) as ch_id
    , toInt64OrZero(JSON_VALUE((op = 'd' ? before : after as ba), '$.chequeitem_id')) as ci_id
    , toInt32OrZero(JSON_VALUE(ba, '$.article_id')) as article_id
    , ts_ms
from integration.loyalty_chequeitem_cur;

/*insert into calc.ci
select
    cityHash64(ci_id, instance_id) as ci_instance_hash
    , cityHash64(ci_id, instance_id, source_id) as key_source_instance_hash --!
    , JSON_VALUE(source, '$.name') as instance_id --!
    , concat(lower(JSON_VALUE(source, '$.schema')), '.', lower(JSON_VALUE(source, '$.table'))) as source_id --!
    , cityHash64(article_id, instance_id) as article_hash
    , toDecimal32OrZero(JSON_VALUE(ba, '$.price'), 2) as price
    , toDecimal32OrZero(JSON_VALUE(ba, '$.quantity'), 2) as quantity
    , toDecimal32OrZero(JSON_VALUE(ba, '$.summdisc'), 2) as summdisc
    , toDecimal32OrZero(JSON_VALUE(ba, '$.summ'), 2) as summ
    , toDecimal32OrZero(JSON_VALUE(ba, '$.discount'), 2) as discount
    , op = 'd' as is_del
    , toInt64OrZero(JSON_VALUE(ba, '$.cheque_id')) as ch_id
    , toInt64OrZero(JSON_VALUE((op = 'd' ? before : after as ba), '$.chequeitem_id')) as ci_id
    , toInt32OrZero(JSON_VALUE(ba, '$.article_id')) as article_id
    , ts_ms
from stage.loyalty_chequeitem_cur;*/

select
    cityHash64(ci_id, instance_id) as ci_instance_hash
    , cityHash64(bonus_id, instance_id, source_id) as key_source_instance_hash
    , JSON_VALUE(source, '$.name') as instance_id
    , concat(lower(JSON_VALUE(source, '$.schema')), '.', lower(JSON_VALUE(source, '$.table'))) as source_id
    , op = 'd' as is_del
    , toInt64OrZero(JSON_VALUE(ba, '$.cheque_id')) as ch_id
    , toInt64OrZero(JSON_VALUE((op = 'd' ? before : after as ba), '$.cheque_item_id')) as ci_id
    , toInt64OrZero(JSON_VALUE(ba, '$.bonus_id')) as bonus_id
    , toInt64OrZero(JSON_VALUE(ba, '$.created_on')) as created_on
    , toDecimal32OrZero(JSON_VALUE(ba, '$.value'), 2) as value
    , lower(JSON_VALUE(ba, '$.isstatus')) = 'true' as is_status
    , toInt32OrZero(JSON_VALUE(ba, '$.campaign_id')) as campaign_id
    , toInt32OrZero(JSON_VALUE(ba, '$.rule_id')) as rule_id
    , JSON_VALUE(ba, '$.operation_type_id') as operation_type_id
from stage.loyalty_bonus_cur
-- where ci_id <> 0;
;

-- {"bonus_id":14239306,"created_on":1714753378046,"value":0.0
-- -- ,"discount":0.0,"start_date":1714753378046,"finish_date":32503680000000,"remainder":0.0
-- ,"cheque_id":56889,"cheque_item_id":null
-- -- ,"parent_type_id":9
-- ,"operation_type_id":"D"
-- -- ,"processed_date":1714753378046,"disposed_date":null,"old_finish_date":null
-- -- ,"source_bonus_id":null,"partition_chequeitem_num":null,"partner_balance":false
-- ,"isstatus":false
-- -- ,"order_id":null,"orderitem_id":null,"organization_id":1
-- ,"campaign_id":34,"rule_id":null
-- -- ,"card_id":557179484,"created_by":2,"finished_by":null,"parent_id":null}
-- ;
