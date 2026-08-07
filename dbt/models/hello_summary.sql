select
    count(*) as total_rows
from {{ ref('hello_model') }}