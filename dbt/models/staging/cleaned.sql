with raw_data as (
    select * from {{ ref('raw') }}
)
select
    id_terc,
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
    case
        when nm_escolaridade in (
            'SEM EXIGENCIA',
            'ALFABETIZADO',
            'ENSINO FUNDAMENTAL INCOMPLETO',
            'ENSINO FUNDAMENTAL COMPLETO',
            'ENSINO MEDIO INCOMPLETO',
            'ENSINO MEDIO COMPLETO',
            'CURSO TECNICO COMPLETO',
            'SUPERIOR INCOMPLETO',
            'SUPERIOR COMPLETO',
            'ESPECIALIZACAO/RESIDENCIA',
            'POS GRADUACAO',
            'MESTRADO',
            'DOUTORADO'
        ) then nm_escolaridade
        else null
    end as nm_escolaridade,
    nullif(nr_jornada, 'NI  ') as nr_jornada,
    nm_unidade_prestacao,
    vl_mensal_salario,
    vl_mensal_custo,
    "Num_Mes_Carga",
    "Mes_Carga",
    "Ano_Carga",
    sg_orgao,
    nm_orgao,
    cd_orgao_siafi,
    cd_orgao_siape
from raw_data