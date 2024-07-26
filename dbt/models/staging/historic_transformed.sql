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
    cast(numero_siafi_gestora as bigint) as numero_siafi_gestora,
    sigla_gestora,
    nome_gestora,
    sigla_orgao_trabalho,
    orgao_trabalho,
    cast(numero_siafi_orgao_trabalho as bigint) as numero_siafi_orgao_trabalho,
    cast(numero_siape_orgao_trabalho as bigint) as numero_siape_orgao_trabalho,
    unidade_trabalho,
    
    -- Contratante
    cast(numero_empresa_terceirizada_cnpj as bigint) as numero_empresa_terceirizada_cnpj,
    empresa_terceirizada_razao_social,

    -- Contrato
    numero_contrato,
    categoria_profissional_cbo,
    cast(jornada_trabalho_horas_semanais as bigint) as jornada_trabalho_horas_semanais,
    escolaridade_exigida,
    cast(valor_reais_mensal_salario as double precision) as valor_reais_mensal_salario,
    cast(valor_reais_mensal_custo as double precision) as valor_reais_mensal_custo,

    -- Contratado
    contratado_cnpj,
    contratado_nome,

    current_timestamp as timestamp_captura
from {{ ref('historic_renamed') }}