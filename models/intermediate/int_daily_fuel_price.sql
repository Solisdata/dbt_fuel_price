SELECT
    CURRENT_DATE() AS date_date
    ,id_station
    ,adresse_complete
    ,marque
    ,nom_station
    ,cp
    ,departement
    ,code_departement
    ,region
    ,commune
    ,geom
    ,s.latitude
    ,s.longitude
    ,type_route
    ,gazole_prix
    ,sp95_prix
    ,e85_prix
    ,gplc_prix
    ,e10_prix
    ,sp98_prix
    -- calcul de la distance de la station  avec le WAGON (47.1894533,-1.535468)
    ,ST_DISTANCE(ST_GEOGPOINT(-1.535468,47.1894533),ST_GEOGPOINT(CAST(s.longitude AS float64),CAST(s.latitude AS float64)))/1000 AS distance_km
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
--- je crée des categories de prix
    ,  CASE
            WHEN gazole_prix BETWEEN 1.1 AND 1.2 THEN "[1.1 - 1.2]"
            WHEN gazole_prix BETWEEN 1.2 AND 1.3 THEN "[1.2 - 1.3]"
            WHEN gazole_prix BETWEEN 1.3 AND 1.4 THEN "[1.3 - 1.4]"
            WHEN gazole_prix BETWEEN 1.4 AND 1.5 THEN "[1.4 - 1.5]"
            WHEN gazole_prix BETWEEN 1.5 AND 1.6 THEN "[1.5 - 1.6]"
            WHEN gazole_prix BETWEEN 1.6 AND 1.7 THEN "[1.6 - 1.7]"
            WHEN gazole_prix BETWEEN 1.7 AND 1.8 THEN "[1.7 - 1.8]"
            WHEN gazole_prix BETWEEN 1.8 AND 1.9 THEN "[1.8 - 1.9]"
            WHEN gazole_prix BETWEEN 1.9 AND 2.0 THEN "[1.9 - 2.0]"
            WHEN gazole_prix >= 2.0 THEN "[2.0+]"
        END AS gazole_prix_interval,

        CASE
            WHEN sp95_prix BETWEEN 1.1 AND 1.2 THEN "[1.1 - 1.2]"
            WHEN sp95_prix BETWEEN 1.2 AND 1.3 THEN "[1.2 - 1.3]"
            WHEN sp95_prix BETWEEN 1.3 AND 1.4 THEN "[1.3 - 1.4]"
            WHEN sp95_prix BETWEEN 1.4 AND 1.5 THEN "[1.4 - 1.5]"
            WHEN sp95_prix BETWEEN 1.5 AND 1.6 THEN "[1.5 - 1.6]"
            WHEN sp95_prix BETWEEN 1.6 AND 1.7 THEN "[1.6 - 1.7]"
            WHEN sp95_prix BETWEEN 1.7 AND 1.8 THEN "[1.7 - 1.8]"
            WHEN sp95_prix BETWEEN 1.8 AND 1.9 THEN "[1.8 - 1.9]"
            WHEN sp95_prix BETWEEN 1.9 AND 2.0 THEN "[1.9 - 2.0]"
            WHEN sp95_prix >= 2.0 THEN "[2.0+]"
        END AS sp95_prix_interval,

        CASE
            WHEN e85_prix BETWEEN 1.1 AND 1.2 THEN "[1.1 - 1.2]"
            WHEN e85_prix BETWEEN 1.2 AND 1.3 THEN "[1.2 - 1.3]"
            WHEN e85_prix BETWEEN 1.3 AND 1.4 THEN "[1.3 - 1.4]"
            WHEN e85_prix BETWEEN 1.4 AND 1.5 THEN "[1.4 - 1.5]"
            WHEN e85_prix BETWEEN 1.5 AND 1.6 THEN "[1.5 - 1.6]"
            WHEN e85_prix BETWEEN 1.6 AND 1.7 THEN "[1.6 - 1.7]"
            WHEN e85_prix BETWEEN 1.7 AND 1.8 THEN "[1.7 - 1.8]"
            WHEN e85_prix BETWEEN 1.8 AND 1.9 THEN "[1.8 - 1.9]"
            WHEN e85_prix BETWEEN 1.9 AND 2.0 THEN "[1.9 - 2.0]"
            WHEN e85_prix >= 2.0 THEN "[2.0+]"
        END AS e85_prix_interval,

        CASE
            WHEN gplc_prix BETWEEN 1.1 AND 1.2 THEN "[1.1 - 1.2]"
            WHEN gplc_prix BETWEEN 1.2 AND 1.3 THEN "[1.2 - 1.3]"
            WHEN gplc_prix BETWEEN 1.3 AND 1.4 THEN "[1.3 - 1.4]"
            WHEN gplc_prix BETWEEN 1.4 AND 1.5 THEN "[1.4 - 1.5]"
            WHEN gplc_prix BETWEEN 1.5 AND 1.6 THEN "[1.5 - 1.6]"
            WHEN gplc_prix BETWEEN 1.6 AND 1.7 THEN "[1.6 - 1.7]"
            WHEN gplc_prix BETWEEN 1.7 AND 1.8 THEN "[1.7 - 1.8]"
            WHEN gplc_prix BETWEEN 1.8 AND 1.9 THEN "[1.8 - 1.9]"
            WHEN gplc_prix BETWEEN 1.9 AND 2.0 THEN "[1.9 - 2.0]"
            WHEN gplc_prix >= 2.0 THEN "[2.0+]"
        END AS gplc_prix_interval,

        CASE
            WHEN e10_prix BETWEEN 1.1 AND 1.2 THEN "[1.1 - 1.2]"
            WHEN e10_prix BETWEEN 1.2 AND 1.3 THEN "[1.2 - 1.3]"
            WHEN e10_prix BETWEEN 1.3 AND 1.4 THEN "[1.3 - 1.4]"
            WHEN e10_prix BETWEEN 1.4 AND 1.5 THEN "[1.4 - 1.5]"
            WHEN e10_prix BETWEEN 1.5 AND 1.6 THEN "[1.5 - 1.6]"
            WHEN e10_prix BETWEEN 1.6 AND 1.7 THEN "[1.6 - 1.7]"
            WHEN e10_prix BETWEEN 1.7 AND 1.8 THEN "[1.7 - 1.8]"
            WHEN e10_prix BETWEEN 1.8 AND 1.9 THEN "[1.8 - 1.9]"
            WHEN e10_prix BETWEEN 1.9 AND 2.0 THEN "[1.9 - 2.0]"
            WHEN e10_prix >= 2.0 THEN "[2.0+]"
        END AS e10_prix_interval,

        CASE
            WHEN sp98_prix BETWEEN 1.1 AND 1.2 THEN "[1.1 - 1.2]"
            WHEN sp98_prix BETWEEN 1.2 AND 1.3 THEN "[1.2 - 1.3]"
            WHEN sp98_prix BETWEEN 1.3 AND 1.4 THEN "[1.3 - 1.4]"
            WHEN sp98_prix BETWEEN 1.4 AND 1.5 THEN "[1.4 - 1.5]"
            WHEN sp98_prix BETWEEN 1.5 AND 1.6 THEN "[1.5 - 1.6]"
            WHEN sp98_prix BETWEEN 1.6 AND 1.7 THEN "[1.6 - 1.7]"
            WHEN sp98_prix BETWEEN 1.7 AND 1.8 THEN "[1.7 - 1.8]"
            WHEN sp98_prix BETWEEN 1.8 AND 1.9 THEN "[1.8 - 1.9]"
            WHEN sp98_prix BETWEEN 1.9 AND 2.0 THEN "[1.9 - 2.0]"
            WHEN sp98_prix >= 2.0 THEN "[2.0+]"
        END AS sp98_prix_interval

FROM {{ ref('stg_stage1__raw_daily_price_dbt') }} d
LEFT JOIN {{ ref('stg_stage1__stations') }} s
    USING (id_station)