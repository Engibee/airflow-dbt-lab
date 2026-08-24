select
    id,
    name,
    email,
    erroproposital
from {{ ref('stg_customers') }}
where email is not null