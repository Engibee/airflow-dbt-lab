select
    id,
    trim(name) as name,
    lower(trim(email)) as email
from {{ source('raw', 'raw_customers') }}