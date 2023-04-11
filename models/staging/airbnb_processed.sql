select *
from {{ source('raw_data', 'airbnb_raw_data') }}