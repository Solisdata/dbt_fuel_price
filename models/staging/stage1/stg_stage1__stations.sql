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
        type_route

    from source

)

select * from renamed
