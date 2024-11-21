-- DATA CLEANING

select *
from layoffs;
-- 1. Remove Duplicates
-- 2. Standardize data
-- 3. Null or blank values
-- 4. Remove not need columns and rows
create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

select *,
row_number() over(
partition by company,location, industry, stage, country, funds_raised_millions, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging
;

with duplicate_cte as
(
select *,
row_number() over(
partition by company,location, industry, stage, country, funds_raised_millions, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging
)
select * 
from duplicate_cte
where row_num > 1;
-- REMOVE DUPLICATES (NONE IN THIS TABLE)

with duplicate_cte as
(
select *,
row_number() over(
partition by company,location, industry, stage, country, funds_raised_millions, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging
)
Delete
from duplicate_cte
where row_num > 1;



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



select *
from layoffs_staging2
where row_num > 1;


insert into layoffs_staging2
select *,
row_number() over(
partition by company,location, industry, stage, country, funds_raised_millions, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging;


delete
from layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2
where row_num > 1;

-- Standardizing data

select company, TRIM(company)
from layoffs_staging2;






