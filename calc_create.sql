create database if not exists calc;

create table calc.ci
(
    key_source_hash UInt64
    , instance_hash UInt64
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
order by key_source_hash;

create materialized view integration.mv_to_calc_ci_from_loyalty_chequeitem_cur to calc.ci as
select
    cityHash64(ci_id, 'partner_id', 'chequeitem') as key_source_hash
    , cityHash64('partner_id') as instance_hash
    , cityHash64(article_id, 'partner_id') as article_hash
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

