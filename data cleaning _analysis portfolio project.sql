use layoffs_project;

-------- DELETING DULPICATE ENTRY FROM DATA----------


select * from layoffs;
select * from layoff1;
create table layoff1 like layoffs;
insert into layoff1 select * from layoffs;

select * ,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off,'date', stage, country,
funds_raised_millions) as row_num
from layoff1;

with duplicate_entry as
(
select * ,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off,'date', stage, country,
funds_raised_millions) as row_num
from layoff1
)
select * from duplicate_entry
where row_num >1;


CREATE TABLE `layoff2` (
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


insert into layoff2
select * ,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off,'date', stage, country,
funds_raised_millions) as row_num
from layoff1;

delete from layoff2
where row_num >1;

select * from layoff2;

-------- STANDERIZING DATA ----------

select company from layoff2;

select distinct(company) from layoff2;
select distinct(trim(company)) from layoff2;
select company, (trim(company)) from layoff2;

update layoff2
set company = trim(company);


set sql_safe_updates =0;
select * from layoff2;

select distinct(industry) from layoff2
order by 1;

update layoff2
set industry = 'crypto'
where industry like 'crypto%';

select distinct(country) from layoff2
order by 1;

update layoff2
set country = 'united states'
where country like 'united states%';

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoff2;

update layoff2
set `date`=
str_to_date(`date`, '%m/%d/%Y') ;

select * from layoff2;

select * from layoff2
where total_laid_off  is null and
 percentage_laid_off is null;

select * from layoff2
where industry is null
or industry = '';

update layoff2
set industry = null
where industry ='';

select *
from layoff2
where company = 'airbnb';

update layoff2 t1
join layoff2 t2
on t1.company = t2.company
set t1. industry= t2.industry
where  (t1.industry is null or t1.industry ='')
and t2.industry is not null;

delete from layoff2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoff2
drop column row_num;


select `date` from layoff2;
update layoff2
set `date` = str_to_date(`date`, '%m/%d/%Y') ;

alter table layoff2
modify column `date` DATE;

select * from layoff2;

-------------- DATA ANALYSIS-------------------
select company, sum(total_laid_off )
from layoff2
group by company
order by 2 desc;

use layoffs_project;
select industry, sum(total_laid_off )
from layoff2
group by industry
order by 2 desc;

select max(date) , min(date)
from layoff2;

select year(`date`), sum(total_laid_off)
from layoff2
group by year(`date`);

select substring(`date`, 1,7) as `month`,sum(total_laid_off)
from layoff2
where substring(`date`, 1,7) is not null
group by `month`
order by 2 desc;

------------------ ROLLING TOTAL ----------------
with Rolling_total as 
(
select substring(`date`, 1,7) as `month`,sum(total_laid_off) as total_off
from layoff2
where substring(`date`, 1,7) is not null
group by `month`
order by 2 desc
)
select `month`, total_off
, sum(total_off) over(order by `month`) as rolling_total
from Rolling_total;


---------ranking----


use layoffs_project;
select company, year(`date`), sum(total_laid_off)
from layoff2
group by company, year(`date`)
order by 3 desc;

WITH company_year(company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoff2
group by company, year(`date`)
order by 3 desc
), company_rank as
(
select * , dense_rank() over(partition by years order by total_laid_off desc) as Ranking
from company_year
where years is not null
)
select * from company_rank
where Ranking <=5;






