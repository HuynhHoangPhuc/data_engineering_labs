select
    c.customer_id                   as customer_key,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    s.state_name                    as customer_state_name,
    s.region                        as customer_region
from {{ ref('stg_olist__customers') }} c
left join {{ ref('brazil_states') }} s
       on s.state_code = c.customer_state
