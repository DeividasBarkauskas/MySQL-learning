-- Data Cleaning

SELECT * FROM layoffs;

-- 1. Remove dublicates 
-- 2. Standartize the data
-- 3. Null Values or blank values
-- 4. Remove Any Columns

-- Sukuriama lentelės 'layoffs' kopija
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * FROM layoffs_staging;

-- Nukopijuojami visi duomenys iš 'layoffs' lentelės į 'layoffs_staging'
INSERT layoffs_staging 
SELECT *
FROM layoffs;

SELECT * FROM layoffs_staging;

-- Sukuriamas naujas stulpelis kuris sunumeruos visus įrašus, taip bus galima identifikuoti dublikatus
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Sukuriamas CTE (Common Table Expression), dublikatams identifikuoti
WITH dublicate_cte AS (
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM dublicate_cte
WHERE row_num > 1; -- Rodomi tik pasikartojantys įrašai (kai eilės numeris didesnis už 1)

-- Sukuriama nauja 'layoffs_staging2' lentelė su papildomu stulpeliu 'row_num'
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
  `row_num`	INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Nukopijuojami visi duomenys iš 'layoffs_staging' į 'layoffs_staging2'
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Ištrinami visi dublikatai 'layoffs_staging2' lentelėje
DELETE FROM layoffs_staging2
WHERE row_num > 1;

SELECT * FROM layoffs_staging2;

-- DATA STANDARDIZE

-- Pašalinami nereikalingi tarpai iš 'company' stulpelio reikšmių
SELECT TRIM(company) new
FROM layoffs_staging2;

-- Atnaujinamas 'company' stulpelis, pašalinant nereikalingus tarpus
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Parodomi unikalūs 'industry' stulpelio įrašai atvirkštine abėcėlės tvarka
select distinct industry
FROM layoffs_staging2
ORDER BY 1 DESC;

-- Parodomi visi įrašai, kur 'industry' stulpelyje yra reikšmė, prasidedanti nuo 'Crypto'
select *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- Atnaujinamas 'industry' stulpelis, kad visi įrašai su reikšme, prasidedančia nuo 'Crypto', būtų nustatyti kaip 'Crypto'
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Parodomi unikalūs 'country' stulpelio įrašai, pašalinant tašką pabaigoje, jei jis yra
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

-- Atnaujinamas 'country' stulpelis, pašalinant tašką pabaigoje
UPDATE layoffs_staging2
SET country  = TRIM(TRAILING '.' FROM country)
WHERE industry LIKE 'United States%';

-- Parodomi 'date' stulpelio įrašai ir konvertuojami į DATE formatą (MM/DD/YYYY)
select `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- Atnaujinamas 'date' stulpelis, konvertuojant jį į DATE formatą
UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

-- Keičiamas 'date' stulpelio tipas į DATE formatą
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Atnaujinamas 'industry' stulpelis, nustatant NULL reikšmes ten, kur yra tuščios reikšmės
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Sujungiami įrašai, siekiant papildyti NULL reikšmes iš kitų eilučių
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Atnaujinamas 'industry' stulpelis, užpildant NULL arba tuščias reikšmes iš kitų eilučių, kur šios reikšmės yra
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Parodomi visi įrašai, kur 'company' stulpelis prasideda nuo 'Bally'
SELECT * 
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

-- Parodomi visi įrašai, kur 'total_laid_off' ir 'percentage_laid_off' stulpeliuose yra NULL reikšmės
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

-- Ištrinami įrašai, kur 'total_laid_off' ir 'percentage_laid_off' stulpeliuose yra NULL reikšmės
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

-- Pašalinamas 'row_num' stulpelis iš 'layoffs_staging2' lentelės, nes jis nebereikalingas
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Parodomi visi įrašai iš 'layoffs_staging2' lentelės
SELECT * 
FROM layoffs_staging2;
