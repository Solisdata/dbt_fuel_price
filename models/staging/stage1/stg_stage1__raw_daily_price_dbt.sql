with

    source as (select * from {{ source("stage1", "raw_daily_price_dbt") }}),

    renamed as (
        -- Rename + types de data + INICAP pour les majuscules 
        select
            cast(id as string) as id_station,
            -- je créé une adresse complete pour la carte
            concat(
                initcap(cast(adresse as string)),
                ', ',
                cast(code_postal as string),
                ' ',
                initcap(cast(ville as string))
            ) as adresse_complete,
            cast(code_postal as string) as cp,
            cast(d__partement as string) as departement,
            -- je corrige le nom ile de france
            case
                when r__gion = 'Île-de-France' then 'IDF' else cast(r__gion as string)
            end as region,
            cast(code_departement as string) as code_departement,
            -- --J'extrait les longitudes et latitudes de geom
            geom,
            cast(split(geom, ',')[safe_offset(0)] as float64) as latitude,
            cast(split(geom, ',')[safe_offset(1)] as float64) as longitude,
            -- info sur les prix
            safe_cast(prix_gazole as float64) as gazole_prix,
            safe_cast(prix_sp95 as float64) as sp95_prix,
            safe_cast(prix_e85 as float64) as e85_prix,
            safe_cast(prix_gplc as float64) as gplc_prix,
            safe_cast(prix_e10 as float64) as e10_prix,
            safe_cast(prix_sp98 as float64) as sp98_prix,
            cast(prix_gazole_mis____jour_le as date) as gazole_maj,
            cast(prix_sp95_mis____jour_le as date) as sp95_maj,
            cast(prix_e85_mis____jour_le as date) as e85_maj,
            cast(prix_gplc_mis____jour_le as date) as gplc_maj,
            cast(prix_e10_mis____jour_le as date) as e10_maj,
            cast(prix_sp98_mis____jour_le as date) as sp98_maj,
            -- info sur les ruptures
            case
                when type_rupture_e10 = 'temporaire'
                then 'rupture temporaire'
                else cast(type_rupture_e10 as string)
            end as e10_rupture_type,

            case
                when type_rupture_sp98 = 'temporaire'
                then 'rupture temporaire'
                else cast(type_rupture_sp98 as string)
            end as sp98_rupture_type,

            case
                when type_rupture_sp95 = 'temporaire'
                then 'rupture temporaire'
                else cast(type_rupture_sp95 as string)
            end as sp95_rupture_type,

            case
                when type_rupture_e85 = 'temporaire'
                then 'rupture temporaire'
                else cast(type_rupture_e85 as string)
            end as e85_rupture_type,

            case
                when type_rupture_gplc = 'temporaire'
                then 'rupture temporaire'
                else cast(type_rupture_gplc as string)
            end as gplc_rupture_type,

            case
                when type_rupture_gazole = 'temporaire'
                then 'rupture temporaire'
                else cast(type_rupture_gazole as string)
            end as gazole_rupture_type,
            -- on met en format binaire les éléments temporaire = 1 , autres = 2
            case
                when type_rupture_e10 = 'temporaire' then 1 else 0
            end as part_rupture_e10,

            case
                when type_rupture_sp98 = 'temporaire' then 1 else 0
            end as part_rupture_sp98,

            case
                when type_rupture_sp95 = 'temporaire' then 1 else 0
            end as part_rupture_sp95,

            case
                when type_rupture_e85 = 'temporaire' then 1 else 0
            end as part_rupture_e85,

            case
                when type_rupture_gplc = 'temporaire' then 1 else 0
            end as part_rupture_gplc,

            case
                when type_rupture_gazole = 'temporaire' then 1 else 0
            end as part_rupture_gazole

        from source
        -- on exclus les stations dont les prix n'ont pas été mis à jours depuis plus
        -- d'un mois
        where
            coalesce(
                prix_gazole_mis____jour_le,
                prix_sp95_mis____jour_le,
                prix_e85_mis____jour_le,
                prix_gplc_mis____jour_le,
                prix_e10_mis____jour_le,
                prix_sp98_mis____jour_le
            )
            >= "2025-02-01"
    )

select *
from renamed
