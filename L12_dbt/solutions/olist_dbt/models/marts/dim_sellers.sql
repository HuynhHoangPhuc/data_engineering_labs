select
    se.seller_id                    as seller_key,
    se.seller_zip_code_prefix,
    se.seller_city,
    se.seller_state,
    s.state_name                    as seller_state_name,
    s.region                        as seller_region
from {{ ref('stg_olist__sellers') }} se
left join {{ ref('brazil_states') }} s
       on s.state_code = se.seller_state
