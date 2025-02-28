-- Exploratory Data Analysis (EDA)

-- Here I am just going to explore the data and find trends or patterns or anything interesting like outliers.


select * 
from layoffs_staging2;


-- Looking at Percentage to see how big these layoffs were

select max(percentage_laid_off), min(percentage_laid_off)
from layoffs_staging2
where percentage_laid_off is not null;


-- Which companies had 1 which is basically 100 percent of their company's laid off

select *from layoffs_staging2
where percentage_laid_off = 1								
order by funds_raised_millions desc nulls last;

-- BritishVolt looks like an EV company, Quibi! I recognize that company - wow raised like 2 billion dollars and went under 


-- Companies which have the highest total layoffs

select company, sum(total_laid_off) 
from layoffs_staging2						
group by company											 
order by 2 desc nulls last
limit 10;


--By location which have the highest

select location, sum(total_laid_off) 
from layoffs_staging2						
group by location											 
order by 2 desc nulls last
limit 10;


--By Industry 

select industry, sum(total_laid_off) 
from layoffs_staging2										 
group by industry
order by 2 desc nulls last;


--By country 

select country, sum(total_laid_off) 					
from layoffs_staging2
group by country
order by 2 desc nulls last;

--By date

select date, sum(total_laid_off) 						
from layoffs_staging2							
group by date
order by 1 desc nulls last;

--By year

select extract(year from date) as year, sum(total_laid_off) 					
from layoffs_staging2
group by year
order by 1 desc nulls last;

--By stage 

select stage, sum(total_laid_off) 						
from layoffs_staging2							
group by stage
order by 2 desc nulls last;

--By month: This is not that readable cause we don't see which year months belonged to so there is another way

select date_trunc('month', date) as month, sum(total_laid_off)																			
from layoffs_staging2												
group by month
order by 1 asc nulls last;

--More readable

select extract('year' from date_trunc('month', date)) as year, extract ('month' from date_trunc('month', date)) as month, sum(total_laid_off) as sum
from layoffs_staging2
group by year, month											
order by year, month asc nulls last;

------------------------------------------------------------------------------------------------------------------------------------------------------

-- Earlier we looked at Companies with the most Layoffs. Now let's look at that per year. 
-- I want to look at 3 top companies per year 

--First, find the sum of the total layoff of a company for each year.

select company, extract('year' from date) as year, sum(total_laid_off) 						
from layoffs_staging2							
group by company, year
order by sum desc nulls last;


--Companies which have the highest total layoffs per year.

with company_year (company, years, total_laid_off) as (
select company, extract('year' from date) as year, sum(total_laid_off) 						
from layoffs_staging2							
group by company, year

), company_year_rank as (

select *, dense_rank() over(partition by years order by total_laid_off desc nulls last) as ranking from company_year
)
select *from company_year_rank
where ranking <= 3 
and years is not null
order by years asc, total_laid_off desc;


-- Rolling Total of Layoffs Per Month.


with cte_rolling as (
select extract('year' from date_trunc('month', date)) as year, extract ('month' from date_trunc('month', date)) as month, sum(total_laid_off) as total_sum
from layoffs_staging2
group by year, month																		
order by year, month asc nulls last
)
select *, sum(total_sum) over(order by year, month) as rolling_total from cte_rolling;












