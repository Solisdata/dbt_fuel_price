
SELECT
    departement,
    region,
    ROUND(AVG(part_rupture_gazole),4) AS part_rupture_gazole,
    ROUND(AVG(part_rupture_e10), 4) AS part_rupture_e10,
    ROUND(AVG(part_rupture_sp98), 4) AS part_rupture_sp98,
    ROUND(AVG(part_rupture_sp95), 4) AS part_rupture_sp95,
    ROUND(AVG(part_rupture_e85), 4) AS part_rupture_e85,
    ROUND(AVG(part_rupture_GPLc), 4) AS part_rupture_GPLc,
    CASE
        WHEN AVG(part_rupture_gazole) < 0.05 THEN 'vert'
        WHEN AVG(part_rupture_gazole) BETWEEN 0.05 AND 0.10 THEN 'jaune'
        WHEN AVG(part_rupture_gazole) BETWEEN 0.10 AND 0.30 THEN 'orange'
        WHEN AVG(part_rupture_gazole) BETWEEN 0.30 AND 0.50 THEN 'rouge'
        ELSE 'noir'
    END AS niveau_alerte_gazole
FROM {{ ref('int_daily_fuel_price') }}
GROUP BY
    departement,
    region




