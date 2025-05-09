--full query
select 
  ip,
  zipcode,
  city,
  state,
  country,
  country_code,
  continent_name,
  latitude,
  longitude,
  timezone,
  timezone_offset,
  timezone_offset_with_dst,
  dst_exists,
  is_dst,
  FORMAT_TIMESTAMP('%F %H:%M:%S', PARSE_TIMESTAMP('%F %H:%M:%E*S%Ez', `current_time`)) as date_utc,
  organization,
  internet_service_provider,
  connection_type
from 
  portfolio-408419.network.ip_data

--world map graph
select 
  IP,
  organization as `Company`,
  internet_service_provider as `Internet Provider`,
  connection_type as `Connection`,
  Timezone,
  Latitude,
  Longitude,
  City,
  State,
  concat(country," (",country_code,")") as `Country`
from 
  portfolio-408419.network.ip_data
where 1=1
  and organization in ($company)
  and country in ($country)
  and continent_name in ($continent)
  and replace(city,"'","") in ($city)
  and replace(state,"'","") in ($state)

--total access graph
select 
  COUNT(*) AS Amount
from 
  `portfolio-408419.network.ip_data`
where 1=1
  and organization in ($company)
  and country in ($country)
  and continent_name in ($continent)
  and replace(city,"'","") in ($city)
  and replace(state,"'","") in ($state)

--top countries graph
select 
  Country,
  count(*) as Amount
from 
  portfolio-408419.network.ip_data
where 1=1
  and organization in ($company)
  and country in ($country)
  and continent_name in ($continent)
  and replace(city,"'","") in ($city)
  and replace(state,"'","") in ($state)
group by
  country
order by 
  Amount desc
limit 5

--top cities graph
select 
  City,
  count(*) as Amount
from 
  portfolio-408419.network.ip_data
where 1=1
  and organization in ($company)
  and country in ($country)
  and continent_name in ($continent)
  and replace(city,"'","") in ($city)
  and replace(state,"'","") in ($state)
group by
  city
order by 
  Amount desc
limit 5

-- access timeline graph
select 
  TIMESTAMP_TRUNC(PARSE_TIMESTAMP('%F %H:%M:%E*S%Ez', `current_time`), HOUR) AS time,
  COUNT(*) AS Amount
from 
  `portfolio-408419.network.ip_data`
where 1=1
  and organization in ($company)
  and country in ($country)
  and continent_name in ($continent)
  and replace(city,"'","") in ($city)
  and replace(state,"'","") in ($state)
group by
  time
order by
  time

--variable continent
select
  distinct continent_name 
from 
  `portfolio-408419.network.ip_data` 

--variable country
select 
  distinct country 
from 
  `portfolio-408419.network.ip_data` 
where 1=1  
  and continent_name in ($continent)

--variable state
select 
  distinct replace(state,"'","") 
from 
  `portfolio-408419.network.ip_data`
where 1=1
  and continent_name in ($continent)
  and country in ($country)

--variable city
select 
  distinct replace(city,"'","") 
from 
  `portfolio-408419.network.ip_data`
where 1=1
  and continent_name in ($continent)
  and country in ($country)
  and replace(state,"'","")  in ($state)

--variable company
select 
  distinct organization 
from 
  `portfolio-408419.network.ip_data` 
where 1=1
  and continent_name in ($continent)
  and country in ($country)
  and replace(state,"'","")  in ($state)
  and replace(city,"'","")  in ($city)
