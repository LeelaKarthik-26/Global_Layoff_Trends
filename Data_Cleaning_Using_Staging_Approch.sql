USE world_job_layoffs;

SHOW tables;

SELECT *
FROM layoffs;

-- STAGING

CREATE table layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- DATA CLEANING
## 1.REMOVE DUPLICATES

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry,total_laid_off, 
					percentage_laid_off, `date`, stage, country,funds_raised_millions) AS row_num
FROM layoffs_staging;

-- CTE common table extration

WITH duplicate_cte AS 
(
    SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry,total_laid_off, 
						percentage_laid_off, `date`, stage, country,funds_raised_millions) AS row_num
	FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num >1;

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

SELECT *
FROM layoffs_staging2;
    
INSERT layoffs_staging2
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry,total_laid_off, 
					percentage_laid_off, `date`, stage, country,funds_raised_millions) AS row_num
	FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;
    
DELETE 
FROM layoffs_staging2
WHERE row_num > 1;
 
-- STANDERDIZING THE DATA

SELECT *
FROM layoffs_staging2;

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;
    
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';    
    
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT `date`
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY `date` date;

SELECT *
FROM layoffs_staging2;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

SELECT *
FROM layoffs_staging2
WHERE total_laid_off is NULL AND percentage_laid_off IS NULL;

-- NULL VLAUES
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT * 
FROM layoffs_staging2
WHERE industry = '' OR industry IS NULL;

UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
		AND t1.location = t2.location
WHERE t1.industry IS NULL 
	AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
	JOIN layoffs_staging2 t2
		ON t1.company = t2.company
			AND t1.location = t2.location
SET t1.industry = t2.industry
	WHERE t1.industry IS NULL 
		AND t2.industry IS NOT NULL;

-- REMOVING UNWANTED ROWS OR COLUMNS

SELECT *
FROM layoffs_staging2;

SELECT * 
FROM  layoffs_staging2
WHERE total_laid_off IS NULL 
	AND percentage_laid_off IS NULL;
    
DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
	AND percentage_laid_off IS NULL;
    
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2; 
## THIS IS THE FINAL CLEAN DATA SET THAT WE CAN USE FOR FUTERE ANALYSIS.













