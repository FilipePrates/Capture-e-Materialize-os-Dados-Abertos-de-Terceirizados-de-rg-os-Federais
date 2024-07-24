select
    -- Chaves primárias em ordem de escopo
    cast(id_terc as bigint) as id_contratado,
    cast("Ano_Carga" as bigint) as ano_disponibilizado,
    "Mes_Carga"as mes_disponibilizado,

    -- Colunas agrupadas e ordenadas por importâncias dos temas

    -- Orgãos Administrativos Relevantes
    sg_orgao_sup_tabela_ug as sigla_orgao_superior_gestora,
    cast(cd_ug_gestora as bigint) as codigo_siafi_gestora,
    sg_ug_gestora as sigla_gestora,
    nm_ug_tabela_ug as nome_gestora,
    sg_orgao as sigla_orgao_trabalho,
    nm_orgao as nome_orgao_trabalho,
    cast(cd_orgao_siafi as bigint) as codigo_siafi_orgao_trabalho,
    cast(cd_orgao_siape as bigint) as codigo_siape_orgao_trabalho,
    nm_unidade_prestacao as desc_unidade_trabalho,
    
    -- Contratante
    cast(nr_cnpj as bigint) as cnpj_empresa_terceirizada,
    nm_razao_social as razao_social_empresa_terceirizada,

    -- Contrato
    nr_contrato as numero_contrato_empresa_terceirizada,
    nm_categoria_profissional as codigo_cbo_categoria_profissional,
    cast(nr_jornada as bigint) as horas_semanais,
    nm_escolaridade as escolaridade_exigida,
    cast(vl_mensal_salario as double precision) as valor_reais_mensal_salario,
    cast(vl_mensal_custo as double precision) as valor_reais_mensal_custo,

    -- Contratado
    nr_cpf as cnpj_contratado,
    nm_terceirizado as nome_contratado,

    current_timestamp as timestamp_captura
from {{ ref('cleaned') }}