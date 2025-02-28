-- Data Cleaning Project with Postgresql
-- Download the dataset from this link --> https://www.kaggle.com/datasets/swaptr/layoffs-2022




--First we need to create a table with the same structure of our dataset to load all of the data into it.
--I created a table all with text datatype just to eliminate the datatype error cause all the null values in dataset was converted into 'NULL' as a text.
--We are going to change the data type later on.

create table layoffs(
	company text null,
	location text null,
	industry text null,
	total_laid_off text null,
	percentage_laid_off text null,
	date text null,
	stage text null,
	country text null, 
	funds_raised_millions text null
);

--Now I need to check in psql terminal if server and client encodings are the same if not we modify it to eliminate any error during the copying of the csv file.
show server_encoding;
show client_encoding;

--My Client encoding was WIN1252 so i changed it to match the server's.
set client_encoding = 'UTF8';

-- now copy the csv file into layoffs table: \copy layoffs from 'C:\Users\user\Downloads\layoffs.csv' with csv header;

-- first thing I want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens.

create table layoffs_staging as 
select *from layoffs;

-- now when I am data cleaning I usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary 

----------------------------------------------------------------------------------------------------------------------------------------------


-- 1. Remove Duplicates

--First let's check for duplicates
--I use CTE to make it readable 
--I Partition the table over each column to make each row unique so i applied row_number() function over each part to assign a new column to check for duplicates 
-- if row number is greater than 1 means that row exist twice (duplicate)


with cte_duplicate as (
	select *,
	row_number() over(partition by company,location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_numbers
	from layoffs_staging
)
select *
from cte_duplicate
where row_numbers >1;

--I want to confirm with 'Casper' if it appears twice.
-- and yes the result is correct now i want to remove those duplicate values.

select *from layoffs_staging
where company = 'Casper';		

--because, We can't delete and update from a CTE so, we need to copy the result into another table.

with cte_duplicate as (
	select *,
	row_number() over(partition by company,location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_numbers
	from layoffs_staging
) 
select * into table layoffs_staging2		
from cte_duplicate;

-- now i can delete from layoffs_staging2.

delete from layoffs_staging2				
where row_numbers > 1;

-- check if they are deleted.
select *from layoffs_staging2;

-------------------------------------------------------------------------------------------------------------------------------------------------

-- 2. Standardize Data

--First, we need to go column by column to check for white spaces, empty values etc..

select *from layoffs_staging2;

--Removing white spaces from column Company.

select company, trim(company) from layoffs_staging2; -- to see the difference 

--Now set the trimmed values.

update layoffs_staging2
set company = trim(company); 

-- if we look at industry it looks like we have some null and empty rows, let's take a look at these:

select distinct industry from layoffs_staging2
order by industry nulls first;

--It seems that those Industries which starts with 'Crypto%' should all be standarized to 'Crypto'.

select *from layoffs_staging2
where industry ilike 'crypto%';

--Let's modify those.

update layoffs_staging2			
set industry = 'Crypto'
where industry ilike 'crypto%';

--Now let's check Country column. It seems we have some trimming to do with the value United States.

select distinct country from layoffs_staging2 
order by country;

--I removed the '.' at the end of the 'United States.'
select distinct country, trim(trailing '.' from country) from layoffs_staging2 
order by country;

--Now, It is time to update the value in the table:

update layoffs_staging2 
set country = trim(trailing '.' from country) 		
where country ilike 'united states%';

------------------------------------------------------------------------------------------------------------

/* when i copied /copy layoff from 'file path' with csv header; null values were automatically converted to 'NULL' text so i had to change them back to NULL so i 
could change the data type of the columns */

-- Start with date column. 
update  layoffs_staging2 		
set date = NULL
where date = 'NULL';

--Change the data type of the column from text to date.

alter table layoffs_staging2 
alter column date type date using date::date;

--changed the data type of the previuos stored records 
update layoffs_staging2
set date = to_date(date, 'MM-DD-YYYY');

--Now, check if we have NULL values. 
select *from layoffs_staging2
where date is null;


-- Now, Let's change 'NULL' text to NULL values in other columns

update  layoffs_staging2 		
set total_laid_off = NULL
where total_laid_off = 'NULL';

update  layoffs_staging2 		
set percentage_laid_off = NULL
where percentage_laid_off = 'NULL';

--Now, change their data types.

alter table layoffs_staging2
alter column total_laid_off type int using total_laid_off::int;

alter table layoffs_staging2
alter column percentage_laid_off type decimal using percentage_laid_off::decimal;

--------------------------------------------------------------------------------------------------------------------------------

-- 3. Look at null values and see what 

--Again, I'm doing changing 'NULL' text to NULL and and if any NULL change to ' '.

select *from layoffs_staging2 
where industry = ' ' or industry is null or industry = 'NULL';

update layoffs_staging2 
set industry = ' '
where industry is null;


update layoffs_staging2 
set industry =  null 
where industry = 'NULL';


--Check for the nulls and empty values.

select *from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

select distinct industry from layoffs_staging2;

select *from layoffs_staging2 
where industry = ' ' or industry is null;

-- we neeed to make sure that we have a value of the same company for those empty or null industries so we can populate into it from those values.

select *from layoffs_staging2 
where company ilike 'airbnb';

select *from layoffs_staging2
where company ilike 'juul';

--it seems we don't have any industries of the 'Bally's interactive company.

select *from layoffs_staging2
where company ilike E'bally\'s interactive';


--I did the inner join on the same table to compare its rows together

select t1.company, t1.location, t1.industry, t2.company, t2.location, t2.industry 
from layoffs_staging2 t1 inner join layoffs_staging2 t2											 
on t1.company = t2.company and t1.location = t2.location									
where (t1.industry is null or t1.industry = ' ') and (t2.industry is not null and t2.industry != ' ');

--Now, I want to populate matched industries into the empty ones.

update layoffs_staging2 t1
set industry = t2.industry 
from layoffs_staging2 t2
where (t1.company = t2.company and t1.location = t2.location) and ((t1.industry is null or t1.industry = ' ') and (t2.industry is not null and t2.industry != ' '));

--Bally's interactive is the only company which i didn't know about its industry 

select *from layoffs_staging2
where industry = ' ' or industry is null; 


--deleting those rows which have total and percentage laid off nulls since we can't do anything with those rows they are useless.

select *from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

delete 
from layoffs_staging2														
where total_laid_off is null and percentage_laid_off is null;

-------------------------------------------------------------------------------------------------------------------------------

-- 4. remove any columns and rows that are not necessary 

--I'm going to drop row_numbers column since i don't need it anymore.

alter table layoffs_staging2
drop  column row_numbers;





