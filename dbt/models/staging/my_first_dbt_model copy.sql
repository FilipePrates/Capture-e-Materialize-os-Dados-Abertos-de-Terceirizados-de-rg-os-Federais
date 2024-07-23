
-- /*
--     Welcome to your first dbt model!
--     Did you know that you can also configure models directly within SQL files?
--     This will override configurations stated in dbt_project.yml

--     Try changing "table" to "view" below
-- */

-- {{ config(materialized='table') }}

-- with source_data as (

--     select 1 as id
--     union all
--     select null as id

-- )

-- select *
-- from source_data

-- /*
--     Uncomment the line below to remove records with null `id` values
-- */

-- -- where id is not null

with source as (

    select
        id_terc,
        sg_orgao_sup_tabela_ug,
        cd_ug_gestora,
        nm_ug_tabela_ug,
        sg_ug_gestora,
        nr_contrato,
        nr_cnpj,
        nm_razao_social,
        nr_cpf,
        nm_terceirizado,
        nm_categoria_profissional,
        nm_escolaridade,
        nr_jornada,
        nm_unidade_prestacao,
        vl_mensal_salario,
        vl_mensal_custo,
        Num_Mes_Carga,
        Mes_Carga,
        Ano_Carga,
        sg_orgao,
        nm_orgao,
        cd_orgao_siafi,
        cd_orgao_siape
    from {{ source('cgu_terceirizados', 'raw.raw') }}

),

staging_casted as (

    select
        cast(id_terc as INT64) as id_terc,
        cast(sg_orgao_sup_tabela_ug as STRING) as sg_orgao_sup_tabela_ug,
        cast(cd_ug_gestora as INT64) as cd_ug_gestora,
        cast(nm_ug_tabela_ug as STRING) as nm_ug_tabela_ug,
        cast(sg_ug_gestora as STRING) as sg_ug_gestora,
        cast(nr_contrato as STRING) as nr_contrato,
        cast(nr_cnpj as STRING) as nr_cnpj,
        cast(nm_razao_social as STRING) as nm_razao_social,
        cast(nr_cpf as STRING) as nr_cpf,
        cast(nm_terceirizado as STRING) as nm_terceirizado,
        cast(nm_categoria_profissional as STRING) as nm_categoria_profissional,
        cast(nm_escolaridade as STRING) as nm_escolaridade,
        cast(nr_jornada as INT64) as nr_jornada,
        cast(nm_unidade_prestacao as STRING) as nm_unidade_prestacao,
        cast(vl_mensal_salario as FLOAT64) as vl_mensal_salario,
        cast(vl_mensal_custo as FLOAT64) as vl_mensal_custo,
        cast(Num_Mes_Carga as INT64) as Num_Mes_Carga,
        cast(Mes_Carga as STRING) as Mes_Carga,
        cast(Ano_Carga as INT64) as Ano_Carga,
        cast(sg_orgao as STRING) as sg_orgao,
        cast(nm_orgao as STRING) as nm_orgao,
        cast(cd_orgao_siafi as INT64) as cd_orgao_siafi,
        cast(cd_orgao_siape as INT64) as cd_orgao_siape
    from source

)

select * from staging_casted;