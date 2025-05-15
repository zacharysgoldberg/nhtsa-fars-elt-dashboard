-- models/marts/accidents/mrt_top_states.sql
{{ config(materialized = 'table') }}

SELECT
    CASE state
        WHEN 'Alabama' THEN 'AL'
        WHEN 'Alaska' THEN 'AK'
        WHEN 'Arizona' THEN 'AZ'
        WHEN 'Arkansas' THEN 'AR'
        WHEN 'California' THEN 'CA'
        WHEN 'Colorado' THEN 'CO'
        WHEN 'Connecticut' THEN 'CT'
        WHEN 'Delaware' THEN 'DE'
        WHEN 'Florida' THEN 'FL'
        WHEN 'Georgia' THEN 'GA'
        WHEN 'Hawaii' THEN 'HI'
        WHEN 'Idaho' THEN 'ID'
        WHEN 'Illinois' THEN 'IL'
        WHEN 'Indiana' THEN 'IN'
        WHEN 'Iowa' THEN 'IA'
        WHEN 'Kansas' THEN 'KS'
        WHEN 'Kentucky' THEN 'KY'
        WHEN 'Louisiana' THEN 'LA'
        WHEN 'Maine' THEN 'ME'
        WHEN 'Maryland' THEN 'MD'
        WHEN 'Massachusetts' THEN 'MA'
        WHEN 'Michigan' THEN 'MI'
        WHEN 'Minnesota' THEN 'MN'
        WHEN 'Mississippi' THEN 'MS'
        WHEN 'Missouri' THEN 'MO'
        WHEN 'Montana' THEN 'MT'
        WHEN 'Nebraska' THEN 'NE'
        WHEN 'Nevada' THEN 'NV'
        WHEN 'New Hampshire' THEN 'NH'
        WHEN 'New Jersey' THEN 'NJ'
        WHEN 'New Mexico' THEN 'NM'
        WHEN 'New York' THEN 'NY'
        WHEN 'North Carolina' THEN 'NC'
        WHEN 'North Dakota' THEN 'ND'
        WHEN 'Ohio' THEN 'OH'
        WHEN 'Oklahoma' THEN 'OK'
        WHEN 'Oregon' THEN 'OR'
        WHEN 'Pennsylvania' THEN 'PA'
        WHEN 'Rhode Island' THEN 'RI'
        WHEN 'South Carolina' THEN 'SC'
        WHEN 'South Dakota' THEN 'SD'
        WHEN 'Tennessee' THEN 'TN'
        WHEN 'Texas' THEN 'TX'
        WHEN 'Utah' THEN 'UT'
        WHEN 'Vermont' THEN 'VT'
        WHEN 'Virginia' THEN 'VA'
        WHEN 'Washington' THEN 'WA'
        WHEN 'West Virginia' THEN 'WV'
        WHEN 'Wisconsin' THEN 'WI'
        WHEN 'Wyoming' THEN 'WY'
        ELSE state
    END AS state_abbrev,
    county,
    city,
    COUNT(*) AS total_accidents,
    SUM(fatalities) AS total_fatalities
FROM {{ ref('stg_accident') }}
WHERE city IS NOT NULL
AND city NOT IN ('NOT APPLICABLE', 'Uknown', 'Not Reported')
GROUP BY state_abbrev, county, city
ORDER BY total_fatalities DESC
LIMIT 10
