with cleaned_data as (
    select * from {{ ref('cleaned') }}
)
select
    cast(nr_jornada as bigint) as id_terc,
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
    cast(nr_jornada as bigint) as nr_jornada,
    nm_unidade_prestacao,
    cast(vl_mensal_salario as double precision) as vl_mensal_salario,
    cast(vl_mensal_custo as double precision) as vl_mensal_custo,
    cast("Num_Mes_Carga" as bigint) as "Num_Mes_Carga",
    "Mes_Carga",
    "Ano_Carga",
    sg_orgao,
    nm_orgao,
    cast(cd_orgao_siafi as bigint) as cd_orgao_siafi,
    cast(cd_orgao_siape as bigint) as cd_orgao_siape
from cleaned_data