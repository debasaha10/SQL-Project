use world_layoffs;

create table layoffs_staging
like layoffs;

insert into layoffs_staging
select * from layoffs;

select * from layoffs_staging;

select *,
row_number() 
over(partition by company, location, industry, total_laid_off, percentage_laid_off,'date', stage, country,
funds_raised_millions) as row_num
from layoffs_staging;

with duplicatate_CTE
as (
select *,
row_number() 
over(partition by company, location, industry, total_laid_off, percentage_laid_off,'date', stage, country,
funds_raised_millions) as row_num
from layoffs_staging)
select * from duplicatate_CTE
where row_num >1;



CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
 `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


insert into layoffs_staging2
select *,
row_number() 
over(partition by company, location, industry, total_laid_off, percentage_laid_off,'date', stage, country,
funds_raised_millions) as row_num
from layoffs_staging;

select * from layoffs_staging2;

delete from layoffs_staging2
where row_num >1;

======================================
standaerdizimg data
select * from layoffs_staging2;

select distinct(trim(company)) from layoffs_staging2;
select company , trim(company) from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct industry from layoffs_staging2
order by 1;

select * from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'crypto'
where industry like 'crypto%';

select distinct location from layoffs_staging2;
select distinct country from layoffs_staging2
order by 1;

select country from layoffs_staging2
where country like 'united states%';

update layoffs_staging2
set country = 'united states'
where country like 'united states%';


select `date`,
STR_TO_DATE(`date`,'%m/%d/%y')
from layoffs_staging2;

update layoffs_staging2
set `date`= STR_TO_DATE(`date`,'%m/%d/%y');

select * from layoffs_staging2;

select * from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2
where industry is null or
industry = '';
update layoffs_staging2
set industry = null
where industry = '';




select *
from layoffs_staging2
where company = 'airbnb';


select t1.industry, t2.industry from 
layoffs_staging2 t1
join layoffs_staging2  t2
on t1.company = t2.company
and t1.location =t2.location
where (t1.industry is null or t1.industry ='')
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2  t2
on t1.company = t2.company
set t1. industry= t2.industry
where  (t1.industry is null or t1.industry ='')
and t2.industry is not null;

select * from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;

select `date` from layoffs_staging2;
select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

select * from layoffs_staging2;

alter table layoffs_staging2
modify column `date` DATE;

