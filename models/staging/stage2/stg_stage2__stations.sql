with

    source as (select * from {{ source("stage2", "stations_2024") }}),

    renamed as (

        select
            cast(id_station as string) as id_station,
            nom_station,
            cast(marque as string) as marque,
            adresse,
            code_postal,
            commune,
            geo_point,
            latitude,
            longitude,
            -- je renomme les routes et autoroutes
            case
                when type_route = 'R'
                then 'Route'
                when type_route = 'A'
                then 'Autoroute'
                else cast(type_route as string)
            end as type_route

        from source

    )

select *
from renamed
