select
    --- Chaves
    id_terc
    "Ano_Carga" as ano_disponibilizado,
    "Mes_Carga" as mes_disponibilizado,

    -- Orgãos Administrativos Relevantes
    sg_orgao_sup_tabela_ug
    cd_ug_gestora 
    sg_ug_gestora
    nm_ug_tabela_ug
    sg_orgao as sigla_orgao_trabalho,
    nm_orgao as nome_orgao_trabalho,
    cd_orgao_siafi as codigo_siafi_orgao_trabalho,
    cd_orgao_siape  as codigo_siape_orgao_trabalho,
    nm_unidade_prestacao
    -- Contratante
    nr_cnpj
    nm_razao_social

    -- Contrato
    nr_contrato
    nm_categoria_profissional
    nm_escolaridade
    nr_jornada 
    vl_mensal_salario
    vl_mensal_custo

    -- Contratado
    nr_cpf
    nm_terceirizado

    current_timestamp as timestamp_captura

    id_terc as id_contratado,
    sg_orgao_sup_tabela_ug as sigla_orgao_superior_gestora,
    cd_ug_gestora as codigo_siafi_gestora,
    nm_ug_tabela_ug as nome_gestora,
    sg_ug_gestora as sigla_gestora,
    nr_contrato as numero_contrato_empresa_terceirizada,
    nr_cnpj as cnpj_empresa_terceirizada,
    nm_razao_social as razao_social_empresa_terceirizada,
    nr_cpf as cnpj_contratado,
    nm_terceirizado as nome_contratado,
    nm_categoria_profissional as codigo_cbo_categoria_profissional,
    nm_escolaridade as escolaridade_exigida,
    nr_jornada as jornada_trabalho_horas_semanais,
    nm_unidade_prestacao as desc_unidade_trabalho,
    vl_mensal_salario as valor_reais_mensal_salario,
    vl_mensal_custo as valor_reais_mensal_custo,
    "Mes_Carga",
    "Ano_Carga",
    sg_orgao,
    nm_orgao,
    cd_orgao_siafi,
    cd_orgao_siape
from {{ ref('cleaned') }}