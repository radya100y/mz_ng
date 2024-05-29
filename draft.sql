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
    , source LowCardinality(String)
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
    , dt_create DateTime64 materialized now64()
) engine = ReplacingMergeTree()
order by (key_id, is_del);

create or replace table draft.keys
(
    key_id UInt64
    , related_key_id UInt64
    , dt_create DateTime materialized now()
) engine = Log;

--=== CI ====
drop table if exists draft.mv_to_stage_from_ci;
create materialized view draft.mv_to_stage_from_ci to draft.stage as
select
    cityHash64(ci_id, 'ci') as key_id
    , ci_id
    , ch_id
    , is_del
    , art_id
    , 'ci' as source
from draft.null_ci;

drop table if exists draft.mv_to_keys_from_ci;
create materialized view draft.mv_to_keys_from_ci to draft.keys as
select
    (arrayJoin(is_del = 1 ? [(h1, h2)] : [(cityHash64(key, 'ci') as h1, cityHash64(rel_key, 'ch') as h2), (h2, h1)]) as tup).1 as key_id
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
drop table if exists draft.mv_to_stage_from_ch;
create materialized view draft.mv_to_stage_from_ch to draft.stage as
select
    cityHash64(ch_id, 'ch') as key_id
    , ch_id
    , is_del
    , dt
    , 'ch' as source
from draft.null_ch;

drop table if exists draft.mv_to_keys_from_ch;
create materialized view draft.mv_to_keys_from_ch to draft.keys as
select
    cityHash64(ch_id, 'ch') as key_id
    , cityHash64(ch_id, 'ch') as related_key_id
from draft.null_ch;

--=== BI ====
drop table if exists draft.mv_to_stage_from_bi;
create materialized view draft.mv_to_stage_from_bi to draft.stage as
select
    cityHash64(bo_id, 'bi') as key_id
    , ch_id
    , ci_id
    , bo_id as bi_id
    , is_del
    , value as bi_value
    , 'bi' as source
from draft.null_bo
where is_del = 1 or (is_del = 0 and ci_id <> 0);


--=== BH ====
drop table if exists draft.mv_to_stage_from_bh;
create materialized view draft.mv_to_stage_from_bh to draft.stage as
select
    cityHash64(bo_id, 'bh') as key_id
    , ch_id
    , bo_id as bh_id
    , is_del
    , value as bh_value
    , 'bh' as source
from draft.null_bo
where is_del = 1 or (is_del = 0 and ci_id = 0);

drop table if exists draft.mv_to_keys_from_bo;
create materialized view draft.mv_to_keys_from_bo to draft.keys as
select
    (arrayJoin([
        (
            cityHash64(
                (
                    multiIf
                    (
                        is_del = 0 and ci_id = 0 , ((bo_id, 'bh'), (ch_id, 'ch'))
                        , is_del = 0 and ci_id <> 0, ((bo_id, 'bi'), (ci_id, 'ci'))
                        , ((bo_id, 'ci'), (bo_id, 'ch'))
                    ) as key
                ).1
            )
            , cityHash64(key.2)
        ), (cityHash64(key.2), cityHash64(key.1))
    ]) as q).1 as key_id
    , q.2 as related_key_id
from draft.null_bo;
-- from (select 1 as bo_id, 0 as is_del, 10 as ci_id, 11 as ch_id union all select 2 as bo_id, 1 as is_del, 0 as ci_id, 0 as ch_id);


--=== EI ====
drop table if exists draft.mv_to_stage_from_ei;
create materialized view draft.mv_to_stage_from_ei to draft.stage as
select
    cityHash64(ea_id, 'ei') as key_id
    , ch_id
    , ci_id
    , ea_id as ei_id
    , is_del
    , value as ei_value
    , 'ei' as source
from draft.null_ea
where is_del = 1 or (is_del = 0 and ci_id <> 0) ;

drop table if exists draft.mv_to_keys_from_ea;
create materialized view draft.mv_to_keys_from_ea to draft.keys as
select
    (arrayJoin([
        (
            cityHash64(
                (
                    multiIf
                    (
                        is_del = 0 and ci_id = 0 , ((ea_id, 'eh'), (ch_id, 'ch'))
                        , is_del = 0 and ci_id <> 0, ((ea_id, 'ei'), (ci_id, 'ci'))
                        , ((ea_id, 'ei'), (ea_id, 'eh'))
                    ) as key
                ).1
            )
            , cityHash64(key.2)
        ), (cityHash64(key.2), cityHash64(key.1))
    ]) as q).1 as key_id
    , q.2 as related_key_id
from draft.null_ea;


--=== EH ====
drop table if exists draft.mv_to_stage_from_eh;
create materialized view draft.mv_to_stage_from_eh to draft.stage as
select
    cityHash64(ea_id, 'eh') as key_id
    , ch_id
    , ea_id as eh_id
    , is_del
    , value as eh_value
    , 'eh' as source
from draft.null_ea
where is_del = 1 or (is_del = 0 and ci_id = 0);


truncate table draft.stage;
truncate table draft.keys;

insert into draft.null_ci values (1, 1, 0, 1);
insert into draft.null_ci values (2, 1, 0, 1);
insert into draft.null_ci values (1, 0, 1, 0);

insert into draft.null_ch values (1, 0);
insert into draft.null_bo values (10, 2, 1, 100, 0);
insert into draft.null_bo values (11, 1, 1, 1000, 0);
insert into draft.null_bo values (11, 0, 0, 0, 1);
insert into draft.null_bo values (14, 1, 1, 8, 0);

insert into draft.null_bo values (15, 0, 1, 44, 0);


insert into draft.null_ea values (110, 1, 1, 1000, 0);
insert into draft.null_ea values (111, 0, 1, 222, 0);
insert into draft.null_ea values (111, 0, 0, 0, 1);

insert into draft.null_ea values (112, 0, 1, 88, 0);
insert into draft.null_ea values (112, 0, 0, 0, 1);


select *, dt_create from draft.stage where key_id in (select key_id from draft.keys);
select * from draft.keys;


with cte as
(
    select
        key_id
        , argMaxIf(tuple(* except (key_id)), dt_create, is_del = 0) as tup
        , argMax(is_del, dt_create) as del
    from draft.stage
    where key_id in
    (
        select distinct arrayJoin([key_id, related_key_id])
        from draft.keys where key_id in
        (
            select distinct arrayJoin([key_id, related_key_id])
            from draft.keys where key_id in
            (
                select distinct arrayJoin([key_id, related_key_id])
                from draft.keys where key_id in
                (
                    select distinct arrayJoin([key_id, related_key_id])
                    from draft.keys where key_id in
                    (
                        select distinct arrayJoin([key_id, related_key_id])
                        from draft.keys
                        where key_id = 8178984314414479468
                    )
                )
            )
        )
    )
    group by key_id
    having ((tup.1 in ('ci', 'ch')) or (tup.1 in ('bi', 'bh', 'ei', 'eh') and del = 0))
    order by tup.1, del
)
select
    ch.ch as ch
    , ci.ci
    , ch.is_del ? ch.is_del : ci.is_del as del
    , ch.dt
    , ci.art
    , ch.bh
    , ci.bi
    , ch.eh
    , ci.ei
from
(
    select
        anyIf(tup.2, tup.1 = 'ci') as ch
        , anyIf(del, tup.1 = 'ci') as is_del
        , tup.3 as ci
        , anyIf(tup.8, tup.1 = 'ci') as art
        , groupArrayIf(tup.11, tup.1 = 'bi') as bi
        , groupArrayIf(tup.13, tup.1 = 'ei') as ei
    from cte
    group by tup.3
) as ci
semi left join
(
    select
        tup.2 as ch
        , anyIf(del, tup.1 = 'ch') as is_del
        , anyIf(tup.9, tup.1 = 'ch') as dt
        , groupArrayIf(tup.10, tup.1 = 'bh') as bh
        , groupArrayIf(tup.12, tup.1 = 'eh') as eh
    from cte
    group by tup.2
) as ch on ch.ch = ci.ch
;


select * from system.warnings;



