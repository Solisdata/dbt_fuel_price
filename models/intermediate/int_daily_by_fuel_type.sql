 SELECT 
        id_station,
        Prix,
        fuel_type,
        adresse_complete,
        marque,
        principales_marques,
        nom_station,
        cp,
        departement,
        code_departement,
        region,
        commune,
        geom,
        latitude,
        longitude,
        type_route,
        distance_km,  -- calcul de la distance de la station avec le WAGON (47.210985300204,-1.550071345527)
        gazole_rupture_temporaire,
        sp95_rupture_temporaire,
        e85_rupture_temporaire,
        gplc_rupture_temporaire,
        e10_rupture_temporaire,
        sp98_rupture_temporaire,
        part_rupture_gazole,
        part_rupture_e10,
        part_rupture_sp98,
        part_rupture_sp95,
        part_rupture_e85,
        part_rupture_glpc
    FROM {{ ref('int_daily_fuel_price') }} e
    UNPIVOT(
        prix FOR fuel_type IN (
            gazole_prix AS 'Gazole',
            sp95_prix   AS 'SP95',
            e85_prix    AS 'E85',
            gplc_prix   AS 'GPLC',
            e10_prix    AS 'E10',
            sp98_prix   AS 'SP98'
        )
    )