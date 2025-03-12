with 

source as (

    select * from {{ source('stage1', 'raw_daily_price_dbt') }}

),

renamed as (

    select
        id as id_station,
        Code_postal AS cp,
        Adresse AS adresse,
        Ville AS ville,
        geom,
        Prix_Gazole_mis____jour_le AS gazole_maj,
        Prix_Gazole AS gazole_prix,
        Prix_SP95_mis____jour_le AS sp95_maj,
        Prix_SP95 AS sp95_prix,
        Prix_E85_mis____jour_le AS e85_maj,
        Prix_E85 AS e85_prix,
        Prix_GPLc_mis____jour_le AS gplc_maj,
        Prix_GPLc AS gplc_prix,
        Prix_E10_mis____jour_le AS e10_prix,
        Prix_SP98_mis____jour_le AS sp98_maj,
        Prix_SP98 AS sp98_prix,
        D__but_rupture_e10__si_temporaire_ AS e10_rupture_debut,
        Type_rupture_e10 AS e10_rupture_type,
        D__but_rupture_sp98__si_temporaire_ AS sp98_rupture_debut,
        Type_rupture_sp98 AS sp98_rupture_type,
        D__but_rupture_sp95__si_temporaire_ AS sp95_rupture_debut,
        Type_rupture_sp95 AS sp95_rupture_type,
        D__but_rupture_e85__si_temporaire_ AS e85_rupture_debut,
        Type_rupture_e85 AS e85_rupture_type,
        D__but_rupture_GPLc__si_temporaire_ AS gplc_rupture_debut,
        Type_rupture_GPLc AS gplc_rupture_type,
        D__but_rupture_gazole__si_temporaire_ AS gazole_rupture_debut,
        Type_rupture_gazole AS gazole_rupture_type,
        Carburants_disponibles AS carburants_disponibles,
        Carburants_indisponibles AS carburants_indisponibles,
        Carburants_en_rupture_temporaire AS carburants_rupture_temporaire,
        Carburants_en_rupture_definitive AS carburants_rupture_definitive,
        D__partement AS departement,
        code_departement AS code_departement,
        R__gion AS region,
        code_region AS code_region,

    from source

)

select * from renamed
