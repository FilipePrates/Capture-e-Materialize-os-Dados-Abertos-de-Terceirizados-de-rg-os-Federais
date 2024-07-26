select
-- Identificador do registro do terceirizado na base de dados original:
    id_terc as id_contratado,

-- Sigla do órgão superior da unidade gestora do terceirizado:
    sg_orgao_sup_tabela_ug as sigla_orgao_superior_gestora,

-- Código da unidade gestora (proveniente do Sistema
-- Integrado de Administração Financeira do Governo Federal - SIAFI) do
-- terceirizado:
    cd_ug_gestora as codigo_siafi_gestora,

-- Nome da unidade gestora do terceirizado:
    nm_ug_tabela_ug as nome_gestora,

--Sigla da unidade gestora do terceirizado:
    sg_ug_gestora as sigla_gestora,
    
--Número do contrato com a empresa terceirizada:
    nr_contrato as numero_contrato_empresa_terceirizada,
    
--Cnpj da empresa terceirizada:
    nr_cnpj as cnpj_empresa_terceirizada,
    
--Razão Social da empresa terceirizada:
    nm_razao_social as razao_social_empresa_terceirizada,
    
--Cpf do terceirizado:
    nr_cpf as cnpj_contratado,
    
--Nome completo do terceirizado:
    nm_terceirizado as nome_contratado,
    
--Código da Classificação Brasileira de
--Ocupações (CBO) e descrição da categoria profissional do terceirizado:
    nm_categoria_profissional as codigo_cbo_categoria_profissional,
    
--Nível de escolaridade exigido pela ocupação do
--terceirizado:
    nm_escolaridade as escolaridade_exigida,
    
--Quantidade de horas semanais de trabalho do terceirizado:
    nr_jornada as jornada_trabalho_horas_semanais,
    
--Descrição da unidade onde o terceirizado
--trabalha:
    nm_unidade_prestacao as desc_unidade_trabalho,
    
--Valor mensal do salário do terceirizado (R$):
    vl_mensal_salario as valor_reais_mensal_salario,
    
--Custo total mensal do terceirizado (R$):
    vl_mensal_custo as valor_reais_mensal_custo,
    
--Mês da carga de dados:
    "Mes_Carga" as mes_disponibilizado,
    
--Ano da carga dos dados:
    "Ano_Carga" as ano_disponibilizado,
    
--Sigla do órgão onde o terceirizado trabalha:
    sg_orgao as sigla_orgao_trabalho,
    
--Nome do órgão onde o terceirizado trabalha:
    nm_orgao as nome_orgao_trabalho,
    
--Código SIAFI do órgão onde o terceirizado trabalha:
    cd_orgao_siafi as codigo_siafi_orgao_trabalho,
    
--Código SIAPE (Sistema de Administração de Pessoal)
--do órgão onde o terceirizado trabalha:
    cd_orgao_siape as codigo_siape_orgao_trabalho
from {{ ref('cleaned_historic') }}


