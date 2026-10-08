select
    p.product_id                                                as product_key,
    coalesce(p.product_category_name, 'unknown')                as category_name_pt,
    coalesce(t.product_category_name_english,
             p.product_category_name, 'unknown')                as category_name,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm * p.product_height_cm * p.product_width_cm as product_volume_cm3,
    case
        when p.product_weight_g is null   then 'unknown'
        when p.product_weight_g < 500     then 'light (<0.5 kg)'
        when p.product_weight_g < 5000    then 'medium (0.5-5 kg)'
        else 'heavy (>5 kg)'
    end                                                         as weight_class
from {{ ref('stg_olist__products') }} p
left join {{ ref('product_category_name_translation') }} t
       on t.product_category_name = p.product_category_name
