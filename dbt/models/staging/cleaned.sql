select
    id_terc,
    trim(both ' ' from replace(sg_orgao_sup_tabela_ug, '<N/I>', 'NI')) as sg_orgao_sup_tabela_ug,
    cd_ug_gestora,
    trim(both ' ' from replace(nm_ug_tabela_ug, '<N/I>', 'NI')) as nm_ug_tabela_ug,
    nullif(trim(both ' ' from replace(sg_ug_gestora, '<N/I>', 'NI')), 'NULL') as sg_ug_gestora,
    nr_contrato,
    trim(both ' ' from replace(nr_cnpj, '<N/I>', 'NI')) as nr_cnpj,
    trim(both ' ' from replace(nm_razao_social, '<N/I>', 'NI')) as nm_razao_social,
    trim(both ' ' from replace(nr_cpf, '<N/I>', 'NI')) as nr_cpf,
    trim(both ' ' from replace(nm_terceirizado, '<N/I>', 'NI')) as nm_terceirizado,
    trim(both ' ' from replace(nm_categoria_profissional, '<N/I>', 'NI')) as nm_categoria_profissional,
    case
        when trim(both ' ' from replace(nm_escolaridade, '<N/I>', 'NI')) in (
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
            'DOUTORADO',
            'NI'
        ) then trim(both ' ' from replace(nm_escolaridade, '<N/I>', 'NI'))
        else null
    end as nm_escolaridade,
    case
        when trim(both ' ' from replace(nr_jornada, '<N/I>', 'NI')) = 'NI' then null
        else nr_jornada
    end as nr_jornada,
    trim(both ' ' from replace(nm_unidade_prestacao, '<N/I>', 'NI')) as nm_unidade_prestacao,
    vl_mensal_salario,
    vl_mensal_custo,
    "Mes_Carga",
    "Ano_Carga",
    case
        when trim(both ' ' from replace(sg_orgao, '<N/I>', 'NI')) = 'NI' then 'NI'
        else trim(both ' ' from replace(sg_orgao, '<N/I>', 'NI'))
    end as sg_orgao,
    case
        when trim(both ' ' from replace(nm_orgao, '<N/I>', 'NI')) = 'NI' then 'NI'
        else trim(both ' ' from replace(nm_orgao, '<N/I>', 'NI'))
    end as nm_orgao,
    nullif(trim(both ' ' from replace(cd_orgao_siafi, '<N/I>', 'NI')), '-2') as cd_orgao_siafi,
    nullif(trim(both ' ' from replace(cd_orgao_siape, '<N/I>', 'NI')), '-2') as cd_orgao_siape
from {{ ref('raw') }}