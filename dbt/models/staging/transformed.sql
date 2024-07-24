select
    -- Chaves primárias em ordem de escopo
    cast(nr_jornada as bigint) as id_terc,
    "Ano_Carga" as ano_disponibilizado,
    "Mes_Carga"as mes_disponibilizado,

    -- Colunas agrupadas e ordenadas por importâncias dos temas

    -- Orgãos administrativos
    sg_orgao_sup_tabela_ug,
    cast(cd_ug_gestora as bigint) as cd_ug_gestora,
    sg_ug_gestora,
    nm_ug_tabela_ug,
    nm_unidade_prestacao,
    sg_orgao,
    nm_orgao,
    cast(cd_orgao_siafi as bigint) as cd_orgao_siafi,
    cast(cd_orgao_siape as bigint) as cd_orgao_siape,
    
    -- Contratante
    cast(nr_cnpj as bigint) as nr_cnpj,
    nm_razao_social,

    -- Contrato
    nr_contrato,
    nm_categoria_profissional,
    cast(nr_jornada as bigint) as nr_jornada,
    nm_escolaridade,
    cast(vl_mensal_salario as double precision) as vl_mensal_salario,
    cast(vl_mensal_custo as double precision) as vl_mensal_custo,

    -- Contratado
    nr_cpf,
    nm_terceirizado,

    current_timestamp as timestamp_captura
from {{ ref('cleaned') }}