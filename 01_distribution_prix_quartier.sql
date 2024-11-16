-- Distribution des prix par quartier

with price_neighbour as (
    select
        neighbourhood_overview as neighbourhood,
        avg(price) as avg_price,
        min(price) as min_price,
        max(price) as max_price,
        count(*) as listings_count
    from {{ ref("curation_listings") }}
    where price is not null
    group by neighbourhood_overview
)

select * from price_neighbour
order by avg_price desc;



-- Version compilée

with price_neighbour as (
    select
        neighbourhood_overview as neighbourhood,
        avg(price) as avg_price,
        min(price) as min_price,
        max(price) as max_price,
        count(*) as listings_count
    from AIRBNB.curation_dev.curation_listings
    where price is not null
    group by neighbourhood_overview
)

select * from price_neighbour
order by avg_price desc;