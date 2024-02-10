-- number of records in both datasets
select count(*) from dataset1;
select count(*) from dataset2;

-- data for state jharkhand and bihar
select * from dataset1 where state in ("jharkhand","bihar");

-- population of India
select sum(population) from dataset2;
select * from dataset2;

-- average growth
select round(avg(growth)*100,2) as Avg_Growth_percent from dataset1;

-- average growth for each state
select state, round(avg(growth)*100,2) as avg_growth_percent from dataset1 group by state;

-- average sex ratio for each state
select state, round(avg(sex_ratio),0) as avg_sex_ratio from dataset1 group by state order by avg_sex_ratio desc;

-- average literacy rate for each state having literacy_rate greater than 90
select state, round(avg(literacy),0) as literacy_rate from dataset1 group by state having literacy_rate > 90 order by literacy_rate desc ;

-- top 3 states with highest growth
with cte1 as (with cte as (select *, avg(growth) over(partition by state) as average from dataset1) select state, average, dense_rank() over (order by average desc) as rnk from cte) select distinct state, round(average*100,2) from cte1 where rnk in (1,2,3);

-- bottom three states with lowest sex ratio
with cte1 as (with cte as (select *, avg(sex_ratio) over (partition by state) as average from dataset1) select state, round(average,2) as average , dense_rank() over (order by average ) as rnk from cte) select distinct state, average from cte1 where rnk in (1,2,3);

-- show top and bottom 3 states when talk about literacy rate
(select state, round(avg(literacy),2) as literacy_rate from dataset1 group by state order by literacy_rate desc limit 3 )
union
(select state, round(avg(literacy),2) as literacy_rate from dataset1 group by state order by literacy_rate limit 3);

-- states whose name starts with a or b
select distinct state from dataset1 where state like "a%" or state like "b%";

create database census;
use census;
select * from dataset1;
select * from dataset2;

-- joining both tables
select * from dataset1 join dataset2 using(district);

-- no_of_males and females for each state respectively
with cte as (select dataset2.state, round(avg(sex_ratio),2) as sex_ratio, round(avg(population),2) as population from dataset2 join dataset1 using(district) group by dataset2.state) select *, (sex_ratio*population)/(100 + sex_ratio) as no_of_males ,population - (sex_ratio*population)/(100 + sex_ratio) as no_of_females from cte;

-- no of literate people in each state 
with cte as (select dataset1.state, round(avg(literacy),2) as literacy, round(avg(population),2) as population from dataset1 join dataset2 using(district) group by dataset1.state) select state, round(literacy*population/100,0) as no_of_literates from cte;

-- previous_population for each state
with cte1 as (with cte as (select dataset1.district, dataset1.state, growth, population from dataset1 join dataset2 using(district)) select district, state, round(100*population/(1+growth),2) as previous_population from cte) select state, round(sum(previous_population),2) as previous_population from cte1 group by state;

-- population vs area ratio for both current and previous times
with cte5 as (select * from (select "1" as keyy ,sum(population) as current_population from dataset2) as abc join
(with cte2 as (with cte1 as (with cte as (select dataset1.district, dataset1.state, growth, population from dataset1 join dataset2 using(district)) select district, state, round(100*population/(1+growth),2) as previous_population from cte) select state, round(sum(previous_population),2) as previous_population from cte1 group by state) select "1" as keyy, sum(previous_population) as previous_population from cte2) as pqr using(keyy) join
(select "1" as keyy, sum(area_km2) as total_area from dataset2) as xyz using(keyy)) select current_population/total_area as "current_population&area_ratio", previous_population/total_area as "previous_population&area_ratio" from cte5;

-- top 3 district with highest literacy rates for each state

with cte as (select *, dense_rank() over (partition by state order by literacy desc) as rnk from dataset1) select * from cte where rnk in (1,2,3);
