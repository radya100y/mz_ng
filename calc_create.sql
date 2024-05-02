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

