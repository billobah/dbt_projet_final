-- Distribution des superhosts par quartier

with superhost_neighbour as (
    select
        host_neighbourhood as neighbourhood,
        sum(case when is_superhost = TRUE then 1 else 0 end) as superhost_count,
        count(*) as total_hosts,
        sum(case when is_superhost = TRUE then 1 else 0 end)::float / count(*) as superhost_ratio
    from {{ ref("curation_hosts") }}
    group by host_neighbourhood
)

select * from superhost_neighbour
order by superhost_ratio desc;


-- version compilée

with superhost_neighbour as (
    select
        host_neighbourhood as neighbourhood,
        sum(case when is_superhost = TRUE then 1 else 0 end) as superhost_count,
        count(*) as total_hosts,
        sum(case when is_superhost = TRUE then 1 else 0 end)::float / count(*) as superhost_ratio
    from AIRBNB.curation_dev.curation_hosts
    group by host_neighbourhood
)

select * from superhost_neighbour
order by superhost_ratio desc;

