-- Relation entre la qualification super-hôte et le prix

with superhost_price as (
    select
        l.price,
        h.is_superhost = TRUE as is_superhost
    from {{ ref('curation_listings') }} l
    join {{ ref('curation_hosts') }} h
    on l.host_id = h.host_id
    where l.price is not null
)
select
    is_superhost,
    avg(price) as avg_price,
    min(price) as min_price,
    max(price) as max_price,
    count(*) as listing_count
from superhost_price
group by is_superhost
order by is_superhost desc;


-- Version compilée

with superhost_price as (
    select
        l.price,
        h.is_superhost = TRUE as is_superhost
    from AIRBNB.curation_dev.curation_listings l
    join AIRBNB.curation_dev.curation_hosts h
    on l.host_id = h.host_id
    where l.price is not null
)
select
    is_superhost,
    avg(price) as avg_price,
    min(price) as min_price,
    max(price) as max_price,
    count(*) as listing_count
from superhost_price
group by is_superhost
order by is_superhost desc;

