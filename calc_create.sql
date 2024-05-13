create database if not exists calc;

drop table if exists calc.ci;
create table calc.ci
(
    key_instance_source_hash UInt64
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
order by key_instance_source_hash;


drop table if exists integration.mv_to_calc_ci_from_loyalty_chequeitem_cur;
create materialized view integration.mv_to_calc_ci_from_loyalty_chequeitem_cur to calc.ci as
select
    cityHash64(ci_id, instance_id, source_id) as key_source_instance_hash
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

select * from calc.ci;

select * from stage.loyalty_chequeitem_cur;

insert into calc.ci
select
    cityHash64(ci_id, instance_id, source_id) as key_source_instance_hash --!
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
from stage.loyalty_chequeitem_cur;

