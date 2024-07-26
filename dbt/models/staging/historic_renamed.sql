select
-- Identificador do registro do terceirizado na base de dados original:
    id_terc as contratado_id,

-- Sigla do órgão superior da unidade gestora do terceirizado:
    sg_orgao_sup_tabela_ug as sigla_orgao_superior_gestora,

-- Código da unidade gestora (proveniente do Sistema
-- Integrado de Administração Financeira do Governo Federal - SIAFI) do
-- terceirizado:
    cd_ug_gestora as numero_siafi_gestora,

-- Nome da unidade gestora do terceirizado:
    nm_ug_tabela_ug as nome_gestora,

--Sigla da unidade gestora do terceirizado:
    sg_ug_gestora as sigla_gestora,
    
--Número do contrato com a empresa terceirizada:
    nr_contrato as numero_contrato,
    
--Cnpj da empresa terceirizada:
    nr_cnpj as numero_empresa_terceirizada_cnpj,
    
--Razão Social da empresa terceirizada:
    nm_razao_social as empresa_terceirizada_razao_social,
    
--Cpf do terceirizado:
    nr_cpf as contratado_cnpj,
    
--Nome completo do terceirizado:
    nm_terceirizado as contratado_nome,
    
--Código da Classificação Brasileira de
--Ocupações (CBO) e descrição da categoria profissional do terceirizado:
    nm_categoria_profissional as categoria_profissional_cbo,
    
--Nível de escolaridade exigido pela ocupação do
--terceirizado:
    nm_escolaridade as escolaridade_exigida,
    
--Quantidade de horas semanais de trabalho do terceirizado:
    nr_jornada as jornada_trabalho_horas_semanais,
    
--Descrição da unidade onde o terceirizado
--trabalha:
    nm_unidade_prestacao as unidade_trabalho,
    
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
    nm_orgao as orgao_trabalho,
    
--Código SIAFI do órgão onde o terceirizado trabalha:
    cd_orgao_siafi as numero_siafi_orgao_trabalho,
    
--Código SIAPE (Sistema de Administração de Pessoal)
--do órgão onde o terceirizado trabalha:
    cd_orgao_siape as numero_siape_orgao_trabalho
from {{ ref('historic_cleaned') }}


