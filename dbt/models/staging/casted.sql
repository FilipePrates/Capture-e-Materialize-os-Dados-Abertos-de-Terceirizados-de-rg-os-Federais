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
        "Num_Mes_Carga",
        "Mes_Carga",
        "Ano_Carga",
        sg_orgao,
        nm_orgao,
        cd_orgao_siafi,
        cd_orgao_siape
    from {{ source('cgu_terceirizados__public', 'raw') }}
),

staging_casted as (
    select
        cast(id_terc as BIGINT) as id_terc,
        cast(sg_orgao_sup_tabela_ug as TEXT) as sg_orgao_sup_tabela_ug,
        cast(cd_ug_gestora as BIGINT) as cd_ug_gestora,
        cast(nm_ug_tabela_ug as TEXT) as nm_ug_tabela_ug,
        cast(sg_ug_gestora as TEXT) as sg_ug_gestora,
        cast(nr_contrato as TEXT) as nr_contrato,
        cast(nr_cnpj as TEXT) as nr_cnpj,
        cast(nm_razao_social as TEXT) as nm_razao_social,
        cast(nr_cpf as TEXT) as nr_cpf,
        cast(nm_terceirizado as TEXT) as nm_terceirizado,
        cast(nm_categoria_profissional as TEXT) as nm_categoria_profissional,
        cast(nm_escolaridade as TEXT) as nm_escolaridade,
        cast(NULLIF("nr_jornada", 'NI  ') as BIGINT) as nr_jornada,
        cast(nm_unidade_prestacao as TEXT) as nm_unidade_prestacao,
        cast(vl_mensal_salario as DOUBLE PRECISION) as vl_mensal_salario,
        cast(vl_mensal_custo as DOUBLE PRECISION) as vl_mensal_custo,
        cast("Num_Mes_Carga" as BIGINT) as Num_Mes_Carga,
        cast("Mes_Carga" as TEXT) as Mes_Carga,
        cast("Ano_Carga" as TEXT) as Ano_Carga,
        cast(sg_orgao as TEXT) as sg_orgao,
        cast(nm_orgao as TEXT) as nm_orgao,
        cast(cd_orgao_siafi as BIGINT) as cd_orgao_siafi,
        cast(cd_orgao_siape as BIGINT) as cd_orgao_siape
    from source
)

select * from staging_casted