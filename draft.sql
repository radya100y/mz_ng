create database if not exists draft;

create or replace table draft.null_ci
(
    ci_id UInt64
    , ch_id UInt64
    , is_del UInt8
    , art_id UInt64
) engine = Null;

create or replace table draft.null_ch
(
    ch_id UInt64
    , is_del UInt8
    , dt DateTime materialized now()
) engine = Null;

create or replace table draft.null_bo
(
    bo_id UInt64
    , ci_id UInt64
    , ch_id UInt64
    , value Int32
    , is_del UInt8
) engine = Null;

create or replace table draft.null_ea
(
    ea_id UInt64
    , ci_id UInt64
    , ch_id UInt64
    , value Int32
    , is_del UInt8
) engine = Null;

create or replace table draft.stage
(
    key_id UInt64
    , ch_id UInt64
    , ci_id UInt64
    , bh_id UInt64
    , bi_id UInt64
    , eh_id UInt64
    , ei_id UInt64
    , art_id UInt64
    , dt DateTime
    , bh_value Int32
    , bi_value Int32
    , eh_value Int32
    , ei_value Int32
    , is_del UInt8
    , dt_create DateTime materialized now()
) engine = ReplacingMergeTree()
order by key_id;

create or replace table draft.keys
(
    key_id UInt64
    , related_key_id UInt64
    , dt_create DateTime materialized now()
) engine = Log;

--=== CI ====
drop table draft.mv_to_stage_from_ci;
create materialized view draft.mv_to_stage_from_ci to draft.stage as
select
    cityHash64(ci_id, 'ci', is_del) as key_id
    , ci_id
    , ch_id
    , is_del
    , art_id
from draft.null_ci;

drop table draft.mv_to_keys_from_ci;
create materialized view draft.mv_to_keys_from_ci to draft.keys as
select
    (arrayJoin(is_del = 1 ? [(h1, h2)] : [(cityHash64(key, 'ci', is_del) as h1, cityHash64(rel_key, 'ch', is_del) as h2), (h2, h1)]) as tup).1 as key_id
    , tup.2 as related_key_id
from
(
    select
        ci_id as key
        , ch_id = 0 and is_del = 1 ? ci_id : ch_id as rel_key
        , is_del
    from draft.null_ci
);

--=== CH ====
drop table draft.mv_to_stage_from_ch;
create materialized view draft.mv_to_stage_from_ch to draft.stage as
select
    cityHash64(ch_id, 'ch', is_del) as key_id
    , ch_id
    , is_del
    , dt
from draft.null_ch;

drop table draft.mv_to_keys_from_ch;
create materialized view draft.mv_to_keys_from_ch to draft.keys as
select
    cityHash64(ch_id, 'ch', is_del) as key_id
    , cityHash64(ch_id, 'ch', is_del) as related_key_id
from draft.null_ch;

--=== BI ====
drop table draft.mv_to_stage_from_bi;
create materialized view draft.mv_to_stage_from_bi to draft.stage as
select
    cityHash64(bo_id, 'bi', is_del) as key_id
    , ch_id
    , ci_id
    , bo_id as bi_id
    , is_del
    , value as bi_value
from draft.null_bo
where is_del = 1 or (is_del = 0 and ci_id <> 0);


--=== BH ====
drop table draft.mv_to_stage_from_bh;
create materialized view draft.mv_to_stage_from_bh to draft.stage as
select
    cityHash64(bo_id, 'bh', is_del) as key_id
    , ch_id
    , bo_id as bh_id
    , is_del
    , value as bh_value
from draft.null_bo
where is_del = 1 or (is_del = 0 and ci_id = 0);

create materialized view draft.mv_to_keys_from_bo to draft.keys as
select
    (arrayJoin([
        (
            cityHash64(
                (
                    multiIf
                    (
                        is_del = 0 and ci_id = 0 , ((bo_id, 'bh', is_del), (ch_id, 'bh', is_del))
                        , is_del = 0 and ci_id <> 0, ((bo_id, 'bi', is_del), (ci_id, 'bi', is_del))
                        , ((bo_id, 'bi', is_del), (bo_id, 'bh', is_del))
                    ) as key
                ).1
            )
            , cityHash64(key.2)
        ), (cityHash64(key.2), cityHash64(key.1))
    ]) as q).1 as key_id
    , q.2 as related_key_id
-- from (select 1 as bo_id, 0 as is_del, 10 as ci_id, 11 as ch_id union all select 2 as bo_id, 1 as is_del, 0 as ci_id, 0 as ch_id);
from draft.null_bo;


--=== EI ====
drop table draft.mv_to_stage_from_ei;
create materialized view draft.mv_to_stage_from_ei to draft.stage as
select
    cityHash64(ea_id, 'ei', is_del) as key_id
    , ch_id
    , ci_id
    , ea_id as ei_id
    , is_del
    , ea_val as ei_value
from draft.null_ea
where is_del = 1 or (is_del = 0 and ci_id <> 0) ;

create materialized view draft.mv_to_keys_from_ea to draft.keys as
select
    (arrayJoin([
        (
            cityHash64(
                (
                    multiIf
                    (
                        is_del = 0 and ci_id = 0 , ((ea_id, 'eh', is_del), (ch_id, 'eh', is_del))
                        , is_del = 0 and ci_id <> 0, ((ea_id, 'ei', is_del), (ci_id, 'ei', is_del))
                        , ((ea_id, 'ei', is_del), (ea_id, 'eh', is_del))
                    ) as key
                ).1
            )
            , cityHash64(key.2)
        ), (cityHash64(key.2), cityHash64(key.1))
    ]) as q).1 as key_id
    , q.2 as related_key_id
from draft.null_ea;


--=== EH ====
drop table draft.mv_to_stage_from_eh;
create materialized view draft.mv_to_stage_from_eh to draft.stage as
select
    cityHash64(ea_id, 'eh', is_del) as key_id
    , ch_id
    , ea_id as eh_id
    , is_del
    , ea_val as eh_value
from draft.null_ea
where is_del = 1 or (is_del = 0 and ci_id = 0);


truncate table draft.stage;
truncate table draft.keys;
insert into draft.null_ci values (1, 1, 0, 1), (2, 1, 0, 1), (1, 0, 1, 0);
insert into draft.null_ch values (1, 0);
insert into draft.null_bo values (11, 1, 1, 1000, 0), (10, 2, 1, 100, 0), (11, 0, 0, 0, 1);
insert into draft.null_bo values (13, 0, 1, 120, 0);
insert into draft.null_ea values (110, 1, 1, 1000, 0);
insert into draft.null_ea values (111, 0, 1, 222, 0), (111, 0, 0, 0, 1);


select * from draft.stage where key_id in (select key_id from draft.keys);
select * from draft.keys;

select *
from draft.keys
where key_id = 5940436581358238122





