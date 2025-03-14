SELECT
    departement,
    region,
    ROUND(AVG(part_rupture_gazole),4) AS part_rupture_gazole
FROM {{ ref('int_daily_fuel_price') }}
GROUP BY
    departement,
    region