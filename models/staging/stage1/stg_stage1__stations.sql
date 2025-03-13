with 

source as (

    select * from {{ source('stage1', 'stations') }}

),

renamed as (

    select
        id_station,
        nom_station,
        marque,
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
