with 

source as (

    select * from {{ source('stage1', 'stations_dbt') }}

),

renamed as (

    select
        CAST(id_station AS STRING) AS id_station,
        nom_station,
        CAST(marque AS STRING) AS marque,
        adresse,
        code_postal,
        commune,
        geo_point,
        latitude,
        longitude,
        gazole,
        sp95,
        sp98,
        gpl,
        e10,
        e85,
        nb_carbu,
        -- je renomme les routes et autoroutes
        CASE
            WHEN type_route = 'R' THEN 'Route'
            WHEN type_route = 'A' THEN 'Autoroute'
            ELSE CAST(type_route AS STRING)
        END AS type_routes


    from source

)

select * from renamed
