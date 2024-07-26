select
-- Identificador do registro do terceirizado nesta base de dados:
    id_terc as id_contratado,
-- Sigla do órgão superior da unidade gestora do terceirizado:
    sg_orgao_sup_tabela_ug as sigla_orgao_superior_gestora,
-- Código da unidade gestora (proveniente do Sistema
-- Integrado de Administração Financeira do Governo Federal - SIAFI) do
-- terceirizado:
    cd_ug_gestora as codigo_siafi_gestora,
-- Nome da unidade gestora do terceirizado
    nm_ug_tabela_ug as nome_gestora,--
    sg_ug_gestora as sigla_gestora,--
    nr_contrato as numero_contrato_empresa_terceirizada,--
    nr_cnpj as cnpj_empresa_terceirizada,--
    nm_razao_social as razao_social_empresa_terceirizada,--
    nr_cpf as cnpj_contratado,--
    nm_terceirizado as nome_contratado,--
    nm_categoria_profissional as codigo_cbo_categoria_profissional,--
    nm_escolaridade as escolaridade_exigida,--
    nr_jornada as jornada_trabalho_horas_semanais,--
    nm_unidade_prestacao as desc_unidade_trabalho,--
    vl_mensal_salario as valor_reais_mensal_salario,--
    vl_mensal_custo as valor_reais_mensal_custo,--
    "Mes_Carga" as mes_disponibilizado,--
    "Ano_Carga" as ano_disponibilizado,--
    sg_orgao as sigla_orgao_trabalho,--
    nm_orgao as nome_orgao_trabalho,--
    cd_orgao_siafi as codigo_siafi_orgao_trabalho,--
    cd_orgao_siape as codigo_siape_orgao_trabalho--
from {{ ref('cleaned') }}