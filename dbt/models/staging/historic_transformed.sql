{{ config(
    materialized='table'
) }}

select
    --- Chaves primárias em ordem de escopo,
    cast(contratado_id as bigint) as contratado_id,
    cast(ano_disponibilizado as bigint) as ano_disponibilizado,
    mes_disponibilizado,

    --- Colunas agrupadas e ordenadas por importâncias dos temas
    -- Orgãos Administrativos Relevantes
    sigla_orgao_superior_gestora,
    cast(codigo_siafi_gestora as bigint) as codigo_siafi_gestora,
    sigla_gestora,
    nome_gestora,
    sigla_orgao_trabalho,
    nome_orgao_trabalho,
    cast(codigo_siafi_orgao_trabalho as bigint) as codigo_siafi_orgao_trabalho,
    cast(codigo_siape_orgao_trabalho as bigint) as codigo_siape_orgao_trabalho,
    desc_unidade_trabalho,
    
    -- Contratante
    cast(cnpj_empresa_terceirizada as bigint) as cnpj_empresa_terceirizada,
    razao_social_empresa_terceirizada,

    -- Contrato
    numero_contrato_empresa_terceirizada,
    codigo_cbo_categoria_profissional,
    cast(jornada_trabalho_horas_semanais as bigint) as jornada_trabalho_horas_semanais,
    escolaridade_exigida,
    cast(valor_reais_mensal_salario as double precision) as valor_reais_mensal_salario,
    cast(valor_reais_mensal_custo as double precision) as valor_reais_mensal_custo,

    -- Contratado
    cnpj_contratado,
    nome_contratado,

    current_timestamp as timestamp_captura
from {{ ref('historic_renamed') }}