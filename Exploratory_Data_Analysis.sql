-- Exploratory Data Analysis

-- Surandame didžiausius "total_laid_off" ir "percentage_laid_off" reikšmes
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

-- Išrenkame įrašus, kuriuose "percentage_laid_off" yra 1, ir rūšiuojame pagal "funds_raised_millions" mažėjimo tvarka
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Surandame bendrą "total_laid_off" kiekį kiekvienai įmonei ir rūšiuojame pagal suma mažėjimo tvarka
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Surandame mažiausią ir didžiausią datą
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- Surandame bendrą "total_laid_off" kiekį pagal industry ir rūšiuojame pagal suma mažėjimo tvarka
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Surandame bendrą "total_laid_off" kiekį pagal šalis ir rūšiuojame pagal suma mažėjimo tvarka
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- Rodyti visus duomenis is table
SELECT *
FROM layoffs_staging2;

-- Surandame bendrą "total_laid_off" kiekį pagal metus ir rūšiuojame pagal metus mažėjimo tvarka
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- Surandame bendrą "total_laid_off" kiekį pagal stage ir rūšiuojame pagal suma mažėjimo tvarka
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- Surandame bendrą "percentage_laid_off" kiekį kiekvienai įmonei ir rūšiuojame pagal suma mažėjimo tvarka
SELECT company, SUM(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Rodyti bendrą "total_laid_off" kiekį pagal data, rūšiuoti pagal datą (atvaizduojant tik metus ir mėnesį) didėjimo tvarka
SELECT SUBSTRING(`date`,1,7) `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

-- Sukuriame laikinas (CTE) duomenų lentelę, kurią pavadiname "Rolling_Total"
-- Naudojame SUBSTRING funkciją, kad išgautume tik mėnesio ir metų dalį iš datos stulpelio.
WITH Rolling_Total AS 
(
SELECT SUBSTRING(`date`,1,7) `MONTH`, -- Surandame pirmus 7 simbolius iš datos stulpelio, kad gautume mėnesio ir metų formatą.
SUM(total_laid_off) AS total_off -- Apskaičiuojame bendrą "total_laid_off" kiekį.
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL -- Filtruojame tik tuos įrašus, kuriuose mėnesio reikšmė nėra NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off, SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;

-- Surandame bendrą "total_laid_off" kiekį kiekvienai įmonei ir rūšiuojame pagal suma (2) mažėjimo tvarka
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Surandame bendrą "total_laid_off" kiekį kiekvienai įmonei pagal metus ir rūšiuojame pagal suma (3) mažėjimo tvarka
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

-- Sukuriame CTE, kuriose apdorojame įmonių duomenis pagal metus
WITH Company_Year (company, years, total_laid_off) AS
(
    -- Išrenkame įmonės pavadinimą, metus (iš datos) ir apskaičiuojame bendrą "total_laid_off" kiekį kiekvienai įmonei per metus.
    -- Grupuoju pagal įmonę ir metus.
    SELECT 
        company, YEAR(`date`) AS years,
        SUM(total_laid_off) AS total_laid_off  -- Apskaičiuojame bendrą "total_laid_off" kiekį kiekvienai įmonei per metus.
    FROM layoffs_staging2
    GROUP BY company, YEAR(`date`)  -- Grupuojame pagal įmonę ir metus.
), Company_Year_Rank AS
(
    -- Iš CTE "Company_Year" sukurime kitą laikinas lentelę, kurioje apskaičiuojame įmonių reitingą.
    -- Naudojame "dense_rank()" funkciją, kad suteiktume įmonėms reitingą pagal bendrą "total_laid_off" kiekį.
    SELECT 
        *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking  -- Apskaičiuojame reitingą pagal "total_laid_off" kiekį, didėjimo tvarka.
    FROM Company_Year
    WHERE years IS NOT NULL
) 
-- Ištraukiame įmones, kurių reitingas yra top 5 pagal kiekį.
SELECT * 
FROM Company_Year_Rank
WHERE Ranking <= 5;
