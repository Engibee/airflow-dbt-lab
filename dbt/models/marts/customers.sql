select
    id,
    name,
    email
from {{ ref('stg_customers') }}
where email is not null