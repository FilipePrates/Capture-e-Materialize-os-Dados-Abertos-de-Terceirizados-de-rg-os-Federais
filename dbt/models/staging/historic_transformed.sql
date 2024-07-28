{{ config(
    materialized='table'
) }}

select
    -- Chaves primárias em ordem de escopo
    cast(contratado_id as bigint) as contratado_id,
    cast(CASE WHEN ano_disponibilizado ~ '^\d+(\.\d+)?$' THEN floor(cast(ano_disponibilizado as float)) ELSE NULL END as bigint) as ano_disponibilizado,
    mes_disponibilizado,

    -- Colunas agrupadas e ordenadas por importâncias dos temas
    -- Orgãos Administrativos Relevantes
    sigla_orgao_superior_gestora,
    cast(CASE WHEN numero_siafi_gestora ~ '^\d+(\.\d+)?$' THEN floor(cast(numero_siafi_gestora as float)) ELSE NULL END as bigint) as numero_siafi_gestora,
    sigla_gestora,
    nome_gestora,
    sigla_orgao_trabalho,
    orgao_trabalho,
    cast(CASE WHEN numero_siafi_orgao_trabalho ~ '^\d+(\.\d+)?$' THEN floor(cast(numero_siafi_orgao_trabalho as float)) ELSE NULL END as bigint) as numero_siafi_orgao_trabalho,
    cast(CASE WHEN numero_siape_orgao_trabalho ~ '^\d+(\.\d+)?$' THEN floor(cast(numero_siape_orgao_trabalho as float)) ELSE NULL END as bigint) as numero_siape_orgao_trabalho,
    unidade_trabalho,
    
    -- Contratante
    cast(CASE WHEN numero_empresa_terceirizada_cnpj ~ '^\d+(\.\d+)?$' THEN floor(cast(numero_empresa_terceirizada_cnpj as float)) ELSE NULL END as bigint) as numero_empresa_terceirizada_cnpj,
    empresa_terceirizada_razao_social,

    -- Contrato
    numero_contrato,
    categoria_profissional_cbo,
    cast(CASE WHEN jornada_trabalho_horas_semanais ~ '^\d+(\.\d+)?$' THEN floor(cast(jornada_trabalho_horas_semanais as float)) ELSE NULL END as bigint) as jornada_trabalho_horas_semanais,
    escolaridade_exigida,
    cast(valor_reais_mensal_salario as double precision) as valor_reais_mensal_salario,
    cast(valor_reais_mensal_custo as double precision) as valor_reais_mensal_custo,
    -- Contratado
    contratado_cpf,
    contratado_nome,

    current_timestamp as timestamp_captura
from {{ ref('historic_renamed') }}