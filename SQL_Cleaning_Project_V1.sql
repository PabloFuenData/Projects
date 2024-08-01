---
# 1. Prepare enviorment

-- 1.1 Create Database 
CREATE DATABASE world_layoffs;

-- 1.2 Load data 


-- 1.3 Create a duplicate db
CREATE TABLE layoffs_staging
LIKE layoffs;

select * 
from layoffs_staging;

-- 1.4 Poblate staging table
insert layoffs_staging
select *
from layoffs;

select * 
from layoffs_staging;

---
# 2. Remove duplicates
-- 2.1 Create a new colum for duplicates
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, country, stage, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- 2.2 Show the duplicates
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, country, stage, funds_raised_millions) AS row_num
FROM layoffs_staging
) 
SELECT *
FROM duplicate_cte
WHERE 	row_num > 1;

-- 2.3 Delete duplicates
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
  `row_num` int -- column added
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * 
from layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, country, stage, funds_raised_millions) AS row_num
FROM layoffs_staging;

select * 
from layoffs_staging2
WHERE row_num > 1;

DELETE 
from layoffs_staging2
WHERE row_num >1;

---
# 3. Stantatize data
-- 3.1 Delete spaces, wrtiting errors and other characters
-- check *company* column
select distinct(company)
from layoffs_staging2;

select company, trim(company)
from layoffs_staging2;

UPDATE layoffs_staging2
SET company = trim(company);

-- check *industry* column
select distinct(industry)
from layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

select distinct(industry)
from layoffs_staging2
ORDER BY 1;

-- check *location* column
select distinct(location)
from layoffs_staging2
ORDER BY 1;

-- check *country* column
select distinct(country)
from layoffs_staging2
ORDER BY 1;

SELECT distinct country, trim(trailing '.' from country)
from layoffs_staging2
ORDER BY 1; 

UPDATE layoffs_staging2
set country = trim(trailing '.' from country)
WHERE country LIKE 'United States%';

select distinct(country)
from layoffs_staging2
ORDER BY 1;

-- 3.2 Convert *date* column to DATE format
select date,
str_to_date(date, '%m/%d/%Y')
from layoffs_staging2;

UPDATE layoffs_staging2
set date = str_to_date(date, '%m/%d/%Y');

select date
from layoffs_staging2
ORDER BY 1;

ALTER TABLE layoffs_staging2
MODIFY COLUMN date DATE;

---
# 4. Null/Blank values
-- 4.1 Convert all *blanks* to *nulls*
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- There're some colums already populated but, others no for the same company. Let's see this example 
SELECT *
FROM layoffs_staging2
WHERE company = "Airbnb";

-- 4.2 We can create a new table so we can join them and populate the missing columns
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

-- 4.3 Populating the empty columns
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry 
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- 4.4 Delete rows with both *total_laid_off* and  *percentage_laid_off* columns with *null* values. 
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

select *
from layoffs_staging2;

---
#5. Final query
-- 5.1 Delete unnecesary columns (*row_num*)
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

select *
from layoffs_staging2;