with source as (
    select * from {{ source('olist', 'products') }}
)
select
    product_id,
    product_category_name,
    -- fix the typos of the original Kaggle column names ("lenght")
    product_name_lenght                             as product_name_length,
    product_description_lenght                      as product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    updated_at
from source
