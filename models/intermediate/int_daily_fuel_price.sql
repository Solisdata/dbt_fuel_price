SELECT 
    id_station
    ,adresse_complete
    ,marque
    ,nom_station
    ,cp
    ,departement
    ,code_departement
    ,region
    ,commune
    ,geom
    ,type_route
    ,gazole_prix
    ,sp95_prix
    ,e85_prix
    ,gplc_prix
    ,e10_prix
    ,sp98_prix
    -- je conserve que les ruptures temporaires
    ,CASE 
        WHEN gazole_rupture_type = 'definitive' THEN NULL 
        ELSE gazole_rupture_type
    END AS gazole_rupture_temporaire

    ,CASE 
        WHEN sp95_rupture_type = 'definitive' THEN NULL 
        ELSE sp95_rupture_type
    END AS sp95_rupture_temporaire

    ,CASE 
        WHEN e85_rupture_type = 'definitive' THEN NULL 
        ELSE e85_rupture_type
    END AS e85_rupture_temporaire

    ,CASE 
        WHEN gplc_rupture_type = 'definitive' THEN NULL 
        ELSE gplc_rupture_type
    END AS gplc_rupture_temporaire

    ,CASE 
        WHEN e10_rupture_type = 'definitive' THEN NULL 
        ELSE e10_rupture_type
    END AS e10_rupture_temporaire

    ,CASE 
        WHEN sp98_rupture_type = 'definitive' THEN NULL 
        ELSE sp98_rupture_type
    END AS sp98_rupture_temporaire

    ,part_rupture_gazole
    ,part_rupture_e10
    ,part_rupture_sp98
    ,part_rupture_sp95
    ,part_rupture_e85
    ,part_rupture_GPLc

    , gazole_maj
    , sp95_maj
    , e85_maj
    , gplc_maj
    , e10_maj
    , sp98_maj

FROM {{ ref('stg_stage1__raw_daily_price_dbt') }} d
LEFT JOIN {{ ref('stg_stage1__stations') }} s
    USING (id_station)