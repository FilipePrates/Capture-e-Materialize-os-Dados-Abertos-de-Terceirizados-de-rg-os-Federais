select
    id_terc,
    sg_orgao_sup_tabela_ug,
    cd_ug_gestora,
    nm_ug_tabela_ug,
    nullif(sg_ug_gestora, 'NULL') as sg_ug_gestora,
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
    -- "Num_Mes_Carga",
    "Mes_Carga",
    "Ano_Carga",
    nullif(sg_orgao, '<N/I>') as sg_orgao,
    nullif(nm_orgao, '<N/I>') as nm_orgao,
    nullif(cd_orgao_siafi, '-2') as cd_orgao_siafi,
    nullif(cd_orgao_siape, '-2') as cd_orgao_siape
from {{ ref('raw') }}