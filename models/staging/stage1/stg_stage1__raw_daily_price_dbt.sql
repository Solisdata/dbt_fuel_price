with 

source as (

    select * from {{ source('stage1', 'raw_daily_price_dbt') }}

),

renamed as (
--Rename + types de data + INICAP pour les majuscules 
    select
        CAST(id AS STRING) AS id_station,
        CAST(Code_postal AS STRING) AS cp,
        INITCAP(CAST(Adresse AS STRING)) AS adresse,
        INITCAP(CAST(Ville AS STRING)) AS ville,
        geom,
        ----J'extrait les longitudes et latitudes de geom
        CAST(SPLIT(geom, ',')[SAFE_OFFSET(0)] AS FLOAT64) AS latitude,
        CAST(SPLIT(geom, ',')[SAFE_OFFSET(1)] AS FLOAT64) AS longitude,
        CAST(Prix_Gazole_mis____jour_le AS DATE) AS gazole_maj,
        SAFE_CAST(Prix_Gazole AS FLOAT64) AS gazole_prix,
        CAST(Prix_SP95_mis____jour_le AS DATE) AS sp95_maj,
        SAFE_CAST(Prix_SP95 AS FLOAT64) AS sp95_prix,
        CAST(Prix_E85_mis____jour_le AS DATE) AS e85_maj,
        SAFE_CAST(Prix_E85 AS FLOAT64) AS e85_prix,
        CAST(Prix_GPLc_mis____jour_le AS DATE) AS gplc_maj,
        SAFE_CAST(Prix_GPLc AS FLOAT64) AS gplc_prix,
        CAST(Prix_E10_mis____jour_le AS DATE) AS e10_prix,
        CAST(Prix_SP98_mis____jour_le AS DATE) AS sp98_maj,
        SAFE_CAST(Prix_SP98 AS FLOAT64) AS sp98_prix,
        CAST(D__but_rupture_e10__si_temporaire_ AS DATE) AS e10_rupture_debut,
        CAST(D__but_rupture_sp98__si_temporaire_ AS DATE) AS sp98_rupture_debut,
        CAST(D__but_rupture_sp95__si_temporaire_ AS DATE) AS sp95_rupture_debut,
        CAST(D__but_rupture_e85__si_temporaire_ AS DATE) AS e85_rupture_debut,
        CAST(D__but_rupture_GPLc__si_temporaire_ AS DATE) AS gplc_rupture_debut,
        CAST(D__but_rupture_gazole__si_temporaire_ AS DATE) AS gazole_rupture_debut,
        CAST(Type_rupture_e10 AS STRING) AS e10_rupture_type,
        CAST(Type_rupture_sp98 AS STRING) AS sp98_rupture_type,
        CAST(Type_rupture_sp95 AS STRING) AS sp95_rupture_type,
        CAST(Type_rupture_e85 AS STRING) AS e85_rupture_type,
        CAST(Type_rupture_GPLc AS STRING) AS gplc_rupture_type,
        CAST(Type_rupture_gazole AS STRING) AS gazole_rupture_type,
        SAFE_CAST(Carburants_disponibles AS INT64) AS carburants_disponibles,
        SAFE_CAST(Carburants_indisponibles AS INT64) AS carburants_indisponibles,
        SAFE_CAST(Carburants_en_rupture_temporaire AS INT64) AS carburants_rupture_temporaire,
        SAFE_CAST(Carburants_en_rupture_definitive AS INT64) AS carburants_rupture_definitive,
        CAST(D__partement AS STRING) AS departement,
        CAST(code_departement AS STRING) AS code_departement,
        CAST(R__gion AS STRING) AS region,
        CAST(code_region AS STRING) AS code_region

    from source
)

select * from renamed
