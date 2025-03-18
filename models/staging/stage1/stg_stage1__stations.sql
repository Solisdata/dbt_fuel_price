with

    source as (select * from {{ source("stage1", "stations_dbt_vf") }}),

    renamed as (

        select
            cast(id_station as string) as id_station,
            nom_station,
            cast(marque as string) as marque,
            "principales marques" as principales_marques,
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
