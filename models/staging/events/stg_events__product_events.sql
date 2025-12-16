with source as (

    select *
    from {{ source('events', 'product_events_raw') }}

),

renamed as (

    select
        -- ajusta estos nombres si tu CSV usa otros encabezados
        event_id,
        account_id,
        user_id,
        event_type,
        event_timestamp_utc

        -- si tienes más columnas útiles en el CSV, las puedes añadir aquí
        -- , some_other_column
    from source

)

select *
from renamed
