-- Data Cleaning

Select *
from world_layoffs.layoffs;









create table layoffs_staging
like layoffs;


Select *
from world_layoffs.layoffs_staging;

insert layoffs_staging
select *
from world_layoffs.layoffs;


select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off,'date') as row_num
from world_layoffs.layoffs_staging;

With duplicate_cte as
(
select *,
row_number() over(
partition by company, Location, 
industry, total_laid_off, percentage_laid_off,'date', stage
, country, funds_raised_millions) as row_num
from world_layoffs.layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;


select *
from world_layoffs.layoffs_staging
where company = 'Casper' ;


With duplicate_cte as
(
select *,
row_number() over(
partition by company, Location, 
industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions) as row_num
from world_layoffs.layoffs_staging
)
delete
from duplicate_cte
where row_num > 1;



CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` bigint DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from world_layoffs.layoffs_staging2
where row_num > 1;
insert into layoffs_staging2
select *,
row_number() over(
partition by company, Location, 
industry, total_laid_off, percentage_laid_off,`date`, stage
, country, funds_raised_millions) as row_num
from world_layoffs.layoffs_staging;




delete
from world_layoffs.layoffs_staging2
where row_num > 1;

select *
from world_layoffs.layoffs_staging2;


-- Standardizing data

select company,trim(company)
from world_layoffs.layoffs_staging2;

Update layoffs_staging2
set company = trim(company);


select distinct industry
from world_layoffs.layoffs_staging2
;

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';


select distinct country, trim(trailing '.' from country)
from world_layoffs.layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

select `date`
from world_layoffs.layoffs_staging2;


UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y')
WHERE `date` = 'NULL';

alter table layoffs_staging2
modify column `date` date;


select * 
from world_layoffs.layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

update world_layoffs.layoffs_staging2
set industry = null
where industry = '';

select *
from world_layoffs.layoffs_staging2;
where industry is null
or industry = '';

select *
from world_layoffs.layoffs_staging2
where company = 'Airbnb';

select *
from world_layoffs.layoffs_staging2 t1
join world_layoffs.layoffs_staging2 t2
     on t1.company = t2.company
where (t1.industry is null or t1.industry ='')
and t2.industry is not null;

update world_layoffs.layoffs_staging2 t1
join world_layoffs.layoffs_staging2 t2
     on t1.company = t2.company
set t1.industry = t2.industry 
where t1.industry is null 
and t2.industry is not null;

select *
from world_layoffs.layoffs_staging2;



select * 
from world_layoffs.layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


delete
from world_layoffs.layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from world_layoffs.layoffs_staging2;

alter table layoffs_staging2
drop column row_num;


