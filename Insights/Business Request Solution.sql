-- 1. 

WITH circulation AS (
 SELECT 
		d.city,
        f.month,
        f.Net_Circulation,
       LAG(f.Net_Circulation) OVER ( PARTITION BY f.City_ID ORDER BY f.Month
         ) AS prev_net_circulation
    FROM fact_print_sales f
    JOIN dim_city d 
        using(City_ID)
),
circulation_diff AS (
    SELECT 
        city,
        Month,
        Net_Circulation,
        prev_net_circulation,
        (Net_Circulation - prev_net_circulation) AS mom_change
    FROM circulation
    WHERE prev_net_circulation IS NOT NULL
      AND (Net_Circulation - prev_net_circulation) < 0
)
SELECT 
    city,
    month,
    Net_Circulation,
    prev_net_circulation,
    mom_change
FROM circulation_diff
ORDER BY mom_change   
LIMIT 3;

-- ----------------------------------------------------
-- ------------------ END of Question 1----------------------------------
-- ------------------------------------------------------

-- 2. 
with yearly_ad_revenue as (
select year, round(sum(ad_revenue_USD), 2) as total_revenue_year
from fact_ad_revenue
group by year
)
select r.year, 
		c.standard_ad_category as category_name,
		round(sum(r.ad_revenue_USD), 2) AS category_revenue,
        total_revenue_year,
		round(sum(r.ad_revenue_USD) / total_revenue_year * 100,2) as pct_of_year_total
from fact_ad_revenue r
join dim_ad_category c 
	on r.ad_category = c.ad_category_id
join yearly_ad_revenue yr on yr.year = r.year
group by  year, c.standard_ad_category, total_revenue_year
-- having sum(r.ad_revenue_USD) / total_revenue_year * 100 > 50
ORDER BY pct_of_year_total, r.year DESC;
-- ----------------------------------------------------
-- ------------------ END of Question 2----------------------------------
-- ------------------------------------------------------

-- 3. 
with efficiency_2024 as (
select city as city_name,
		sum(f.`copies sold`) as copies_printed_2024,
        sum(f.net_circulation) as net_circulation_2024,
        sum(net_circulation) * 1.0 / sum(`copies sold`)  as efficiency_ratio,
		dense_rank() over(order by 
          sum(f.net_circulation) * 1.0 / SUM(f.`Copies Sold`)  DESC
          ) as efficiency_rank_2024
from fact_print_sales f
join dim_city using(city_id)
where f.month >= '2024-01-01'
group by city)
select * 
from efficiency_2024
where efficiency_rank_2024 <= 5;

-- ----------------------------------------------------
-- ------------------ END of Question 3----------------------------------
-- ------------------------------------------------------

-- 4. 
with Q1_2021_internet_penetration as (
select city, 
	   internet_penetration as internet_penetration_Q1_2021
from fact_city_readiness cr
join dim_city using(city_id)
where quarter = "2021-Q1" 
),
Q4_2021_internet_penetration as (
select city, 
	   internet_penetration as internet_penetration_Q4_2021
from fact_city_readiness cr
join dim_city using(city_id)
where quarter = "2021-Q4" 
)
select  city, 
	   internet_penetration_Q1_2021,
       internet_penetration_Q4_2021,
       internet_penetration_Q4_2021 - internet_penetration_Q1_2021 as delta_internet_rate
from Q1_2021_internet_penetration
join Q4_2021_internet_penetration using(city)
order by delta_internet_rate desc
limit 1;

-- ----------------------------------------------------
-- ------------------ END of Question 4----------------------------------
-- ------------------------------------------------------

-- 5. 

with city_yearly_net_circulation as (
select city,
		f.year,
		sum(net_circulation) as yearly_net_circulation,
        LAG(sum(Net_Circulation)) OVER ( PARTITION BY city ORDER BY f.year
         ) as PY_net_circulation
from fact_print_sales f
join dim_city c using(city_id)
group by city, f.year
),
city_yearly_ad_revenue as (
select city,
		r.year,
        sum(ad_revenue_USD) as yearly_ad_revenue_USD,
          LAG(sum(ad_revenue_USD)) OVER ( PARTITION BY city ORDER BY year
         ) as PY_ad_revenue_USD
from fact_ad_revenue r
join dim_city c using(city_id)
group by city, r.year
)
select  city, 
		year,
		yearly_net_circulation,
        round(yearly_ad_revenue_USD, 2) as yearly_ad_revenue_USD,
        Case 
			When PY_net_circulation is null then null  		-- this is when the year is 2019, because we do not have a year before 2019
            else
				case
					when (yearly_net_circulation - PY_net_circulation) < 0 then "YES"
					else "NO"
				end
		end as is_declining_print, 
        case 
			when PY_net_circulation is null then null 	-- this is when the year is 2019, because we do not have a year before 2019
            else
				Case 
					when (yearly_ad_revenue_USD - PY_ad_revenue_USD) < 0 then "YES"
					else "NO"
				end
		END as is_declining_ad_revenue_USD,
        
        -- instead of writing another CTE to say 'if both are YES, return YES ', i just added one more condition.
        case 
            when year = 2019 then null
            else
				case
					when (yearly_net_circulation - PY_net_circulation) < 0 and 
                         (yearly_ad_revenue_USD - PY_ad_revenue_USD) < 0 then "YES"
					else "NO"
				end
		end as is_declining_both
from city_yearly_net_circulation
join city_yearly_ad_revenue using(city, year)
having is_declining_both = "yes";

-- ----------------------------------------------------
-- ------------------ END of Question 5----------------------------------
-- ------------------------------------------------------

-- 6

with engagement_outlier as (
select city,
		avg(literacy_rate + smartphone_penetration + internet_penetration) / 3 as readiness_score_2021,
        (sum(downloads_or_accesses) / sum(users_reached)) * 100 as engagement_score_2021,
        dense_rank() over(order by
        avg(literacy_rate + smartphone_penetration + internet_penetration) / 3 desc
        ) as readiness_rank_desc,
        dense_rank() over(order by
        (sum(downloads_or_accesses) / sum(users_reached)) * 100 ASC
        ) as engagement_rank_asc
from dim_city c
join fact_city_readiness cr using(city_id)
join fact_digital_pilot using(city_id)
where cr.quarter like "2021-%"
group by city)
select city, 
		round(readiness_score_2021, 2) as readiness_score_2021, 
        round(engagement_score_2021,2) as engagement_score_2021,
        readiness_rank_desc, 
        engagement_rank_asc,
		case
			when readiness_rank_desc = 1 and engagement_rank_asc <= 3 then "YES"
            else "NO"
		End as is_outlier
from engagement_outlier
order by is_outlier desc
limit 1;

-- ----------------------------------------------------
-- ------------------ END of Question 6 and solutions----------------------------------
-- ------------------------------------------------------
