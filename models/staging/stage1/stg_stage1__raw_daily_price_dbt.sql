with 

source as (

    select * from {{ source('stage1', 'raw_daily_price_dbt') }}

),

renamed as (
--Rename + types de data + INICAP pour les majuscules 
    select
        CAST(id AS STRING) AS id_station,
        -- je créé une adresse complete pour la carte
        CONCAT(INITCAP(CAST(Adresse AS STRING)), ', ', CAST(Code_postal AS STRING), ' ', INITCAP(CAST(Ville AS STRING))) AS adresse_complete,
        CAST(Code_postal AS STRING) AS cp,
        CAST(D__partement AS STRING) AS departement,
        -- je corrige le nom ile de france
        CASE WHEN R__gion = 'Île-de-France' THEN 'IDF' ELSE CAST(R__gion AS STRING) END AS region,
        CAST(code_departement AS STRING) AS code_departement,
        ----J'extrait les longitudes et latitudes de geom
        CAST geom AS FLOAT64,
        CAST(SPLIT(geom, ',')[SAFE_OFFSET(0)] AS FLOAT64) AS latitude,
        CAST(SPLIT(geom, ',')[SAFE_OFFSET(1)] AS FLOAT64) AS longitude,
        -- info sur les prix
        SAFE_CAST(Prix_Gazole AS FLOAT64) AS gazole_prix,
        SAFE_CAST(Prix_SP95 AS FLOAT64) AS sp95_prix,
        SAFE_CAST(Prix_E85 AS FLOAT64) AS e85_prix,
        SAFE_CAST(Prix_GPLc AS FLOAT64) AS gplc_prix,
        SAFE_CAST(Prix_E10 AS FLOAT64) AS e10_prix,
        SAFE_CAST(Prix_SP98 AS FLOAT64) AS sp98_prix,
        CAST(Prix_Gazole_mis____jour_le AS DATE) AS gazole_maj,
        CAST(Prix_SP95_mis____jour_le AS DATE) AS sp95_maj,
        CAST(Prix_E85_mis____jour_le AS DATE) AS e85_maj,
        CAST(Prix_GPLc_mis____jour_le AS DATE) AS gplc_maj,
        CAST(Prix_E10_mis____jour_le AS DATE) AS e10_maj,
        CAST(Prix_SP98_mis____jour_le AS DATE) AS sp98_maj,
         -- info sur les ruptures
        CASE 
            WHEN Type_rupture_e10 = 'temporaire' THEN 'rupture temporaire' 
            ELSE CAST(Type_rupture_e10 AS STRING) 
        END AS e10_rupture_type,
        
        CASE 
            WHEN Type_rupture_sp98 = 'temporaire' THEN 'rupture temporaire' 
            ELSE CAST(Type_rupture_sp98 AS STRING) 
        END AS sp98_rupture_type,
        
        CASE 
            WHEN Type_rupture_sp95 = 'temporaire' THEN 'rupture temporaire' 
            ELSE CAST(Type_rupture_sp95 AS STRING) 
        END AS sp95_rupture_type,
        
        CASE 
            WHEN Type_rupture_e85 = 'temporaire' THEN 'rupture temporaire' 
            ELSE CAST(Type_rupture_e85 AS STRING) 
        END AS e85_rupture_type,
        
        CASE 
            WHEN Type_rupture_GPLc = 'temporaire' THEN 'rupture temporaire' 
            ELSE CAST(Type_rupture_GPLc AS STRING) 
        END AS gplc_rupture_type,
        
        CASE 
            WHEN Type_rupture_gazole = 'temporaire' THEN 'rupture temporaire' 
            ELSE CAST(Type_rupture_gazole AS STRING) 
        END AS gazole_rupture_type,
        -- on met en format binaire les éléments temporaire = 1 , autres = 2
        CASE 
            WHEN Type_rupture_e10 = 'temporaire' THEN 1
            ELSE 0
        END AS part_rupture_e10,
    
        CASE 
            WHEN Type_rupture_sp98 = 'temporaire' THEN 1
            ELSE 0
        END AS part_rupture_sp98,
    
        CASE 
            WHEN Type_rupture_sp95 = 'temporaire' THEN 1
            ELSE 0
        END AS part_rupture_sp95,
    
        CASE 
            WHEN Type_rupture_e85 = 'temporaire' THEN 1
            ELSE 0
        END AS part_rupture_e85,
    
        CASE 
            WHEN Type_rupture_GPLc = 'temporaire' THEN 1
            ELSE 0
        END AS part_rupture_GPLc,
    
        CASE 
            WHEN Type_rupture_gazole = 'temporaire' THEN 1
            ELSE 0
        END AS part_rupture_gazole
    
    from source
)

select * from renamed
