-- models/marts/accidents/mrt_top_states.sql
{{ config(materialized = 'table') }}

select
    case state
        when 'Alabama' then 'AL'
        when 'Alaska' then 'AK'
        when 'Arizona' then 'AZ'
        when 'Arkansas' then 'AR'
        when 'California' then 'CA'
        when 'Colorado' then 'CO'
        when 'Connecticut' then 'CT'
        when 'Delaware' then 'DE'
        when 'Florida' then 'FL'
        when 'Georgia' then 'GA'
        when 'Hawaii' then 'HI'
        when 'Idaho' then 'ID'
        when 'Illinois' then 'IL'
        when 'Indiana' then 'IN'
        when 'Iowa' then 'IA'
        when 'Kansas' then 'KS'
        when 'Kentucky' then 'KY'
        when 'Louisiana' then 'LA'
        when 'Maine' then 'ME'
        when 'Maryland' then 'MD'
        when 'Massachusetts' then 'MA'
        when 'Michigan' then 'MI'
        when 'Minnesota' then 'MN'
        when 'Mississippi' then 'MS'
        when 'Missouri' then 'MO'
        when 'Montana' then 'MT'
        when 'Nebraska' then 'NE'
        when 'Nevada' then 'NV'
        when 'New Hampshire' then 'NH'
        when 'New Jersey' then 'NJ'
        when 'New Mexico' then 'NM'
        when 'New York' then 'NY'
        when 'North Carolina' then 'NC'
        when 'North Dakota' then 'ND'
        when 'Ohio' then 'OH'
        when 'Oklahoma' then 'OK'
        when 'Oregon' then 'OR'
        when 'Pennsylvania' then 'PA'
        when 'Rhode Island' then 'RI'
        when 'South Carolina' then 'SC'
        when 'South Dakota' then 'SD'
        when 'Tennessee' then 'TN'
        when 'Texas' then 'TX'
        when 'Utah' then 'UT'
        when 'Vermont' then 'VT'
        when 'Virginia' then 'VA'
        when 'Washington' then 'WA'
        when 'West Virginia' then 'WV'
        when 'Wisconsin' then 'WI'
        when 'Wyoming' then 'WY'
        else state
    end as state_abbrev,
    county,
    city,
    count(*) as total_accidents,
    sum(fatalities) as total_fatalities
from {{ ref('stg_accident') }}
where city is not null
and city not in ('NOT APPLICABLE', 'Unknown', 'Not Reported')
group by state_abbrev, county, city
order by total_fatalities desc
limit 10
