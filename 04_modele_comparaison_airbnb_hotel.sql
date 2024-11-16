/*
Développer un modèle qui puisse:
- déduire si les touristes tendent à préférer Airbnb aux hôtels
- montrer l'évolution de cette tendance au long des années
*/


-- Etape 1. Création d'un modèle DBT dans CURATION_DEV qui référence "CURATION_REVIEWS"
-- et qui agrège les reviews par année (curation_dev.curation_reviews_count_per_year)

{{
    config(
        schema=var("curation_schema", "curation")
    )
}}

with listing_reviews_by_year as (
    select
        year(review_date) as review_year,
        count(*) as number_reviews
    from {{ ref('curation_reviews') }}
    group by year(review_date)
)

select * 
from listing_reviews_by_year
order by review_year desc


-- Voici le code compilé de ce modèle 

with listing_reviews_by_year as (
    select
        year(review_date) as review_year,
        count(*) as number_reviews
    from AIRBNB.curation_dev.curation_reviews
    group by year(review_date)
)

select * 
from listing_reviews_by_year
order by review_year desc


-------------------------------------------------------------------------------------------
/*
Etape 2. Création d'un modèle DBT qui permet :
a) d'estimer la proportion de touristes qui préfèrent Airbnb aux hôtels
b) d'évaluer la tendance annuelle

J'obtiens des ratios de préférence très faibles.
Ma conclusion est que les touristes préfèreraient les hôtels à AIRBNB :
*/


-- Voici le code compilé du modèle "curation_tendance_airbnb_vs_hotels"

with tourists_per_year as (
    select * from AIRBNB.curation_dev.curation_tourists_per_year
),

reviews_per_year as (
    select * from AIRBNB.curation_dev.curation_reviews_count_per_year
),

combined_tourists_reviews as (
    select
        year(t.year) as year, -- Ensures `t.year` is converted to numeric year
        t.tourists,
        r.number_reviews,
        (cast(r.number_reviews as float) / t.tourists) as airbnb_preference_ratio
    from tourists_per_year t
    left join reviews_per_year r
    on year(t.year) = r.review_year -- Matches both as numeric years
)

select
    year,
    tourists,
    number_reviews,
    airbnb_preference_ratio,
    case 
        when coalesce(airbnb_preference_ratio, 0) >= 0.6 then 'High Airbnb Preference'
        when coalesce(airbnb_preference_ratio, 0) >= 0.3 then 'Moderate Airbnb Preference'
        else 'Low Airbnb Preference'
    end as preference_category
from combined_tourists_reviews
order by year


-- Voici le code SQL de l'analyse de tendance (dans le dossier analyses de dbt)

with annual_trend as (
    select 
        year,
        avg(airbnb_preference_ratio) over (order by year) as avg_airbnb_preference_ratio
    from AIRBNB.curation_dev.curation_tendance_airbnb_vs_hotels
)

select * from annual_trend;
