select
    id_terc,
    nullif(trim(both ' ' from replace(sg_orgao_sup_tabela_ug, '<N/I>', 'Não Informado')), 'NULL') as sg_orgao_sup_tabela_ug,
    cd_ug_gestora,
    nullif(trim(both ' ' from replace(nm_ug_tabela_ug, '<N/I>', 'Não Informado')), 'NULL') as nm_ug_tabela_ug,
    nullif(trim(both ' ' from replace(sg_ug_gestora, '<N/I>', 'Não Informado')), 'NULL') as sg_ug_gestora,
    nr_contrato,
    nullif(trim(both ' ' from replace(nr_cnpj, '<N/I>', 'Não Informado')), 'NULL') as nr_cnpj,
    nullif(trim(both ' ' from replace(nm_razao_social, '<N/I>', 'Não Informado')), 'NULL') as nm_razao_social,
    nullif(trim(both ' ' from replace(nr_cpf, '<N/I>', 'Não Informado')), 'NULL') as nr_cpf,
    nullif(trim(both ' ' from replace(nm_terceirizado, '<N/I>', 'Não Informado')), 'NULL') as nm_terceirizado,
    nullif(trim(both ' ' from replace(nm_categoria_profissional, '<N/I>', 'Não Informado')), 'NULL') as nm_categoria_profissional,
    case
        when nm_escolaridade in (
            'NAO SABE LER/ESCREVER',
            'SEM EXIGENCIA',
            'ALFABETIZADO',
            'ENSINO FUNDAMENTAL INCOMPLETO',
            'ENSINO FUNDAMENTAL COMPLETO',
            'ENSINO MEDIO INCOMPLETO',
            'ENSINO MEDIO COMPLETO',
            'CURSO TECNão InformadoCO COMPLETO',
            'SUPERIOR INCOMPLETO',
            'SUPERIOR COMPLETO',
            'ESPECIALIZACAO/RESIDENCIA',
            'POS GRADUACAO',
            'MESTRADO',
            'DOUTORADO',
            'Não Informado'
        ) then nm_escolaridade
        else null
    end as nm_escolaridade,
    nullif(nullif(trim(both ' ' from replace(nr_jornada, '<N/I>', 'Não Informado')), 'NULL'), 'Não Informado') as nr_jornada,
    nullif(trim(both ' ' from replace(nm_unidade_prestacao, '<N/I>', 'Não Informado')), 'NULL') as nm_unidade_prestacao,
    vl_mensal_salario,
    vl_mensal_custo,
    -- "Num_Mes_Carga",
    "Mes_Carga",
    "Ano_Carga",
    nullif(trim(both ' ' from replace(sg_orgao, '<N/I>', 'Não Informado')), 'NULL') as sg_orgao,
    nullif(trim(both ' ' from replace(nm_orgao, '<N/I>', 'Não Informado')), 'NULL') as nm_orgao,
    nullif(nullif(trim(both ' ' from replace(cd_orgao_siafi, '<N/I>', 'Não Informado')), 'NULL'), '-2') as cd_orgao_siafi,
    nullif(nullif(trim(both ' ' from replace(cd_orgao_siape, '<N/I>', 'Não Informado')), 'NULL'), '-2') as cd_orgao_siape
from {{ ref('raw') }}