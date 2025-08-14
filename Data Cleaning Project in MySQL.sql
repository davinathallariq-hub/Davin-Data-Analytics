-- #PROJECT1
-- DATA CLEANING 
-- DATA CLEANING is where we try to get data in a more usable format. so we fix a lot of issues in a raw data-
-- -so when we start creating visualizations or start using it in our product that the data is actually useful and there is 0 issue.

-- #1 Create Database with create new schema and import the data
SELECT * 
FROM layoffs
;

-- #2 Start to Exploratory Data Analysis with this multiple steps
-- 		a. Remove Duplicates (if any)
-- 		b. Standardize the data
-- 		c. Null Values or Blank Values
-- 		d. Remove any columns that unnecessary OR (create Staging to keep the data raw complete)

-- CREATE Staging
Create Table layoffs_staging
like layoffs
;
 
INSERT layoffs_staging
SELECT * 
FROM layoffs
;

-- a. REMOVE DUPLICATES
-- bikin filter pake row_num
SELECT *,
ROW_NUMBER () OVER (PARTITION BY 
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
;
-- kita nyari duplicate lewat cte
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER () OVER (PARTITION BY 
company, location, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
Where row_num > 1
;
SELECT *
FROM layoffs_staging
Where company = 'Yahoo'
;


-- habis tu kita coba hapus data duplikatnya lewat buat table, cara ini beda dari microsoft mysql
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` double DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL, 
  `row_num`INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER () OVER (PARTITION BY 
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;
-- nah habis tu langsung kita hapus duplicate nya
DELETE
FROM layoffs_staging2
Where row_num > 1;
SELECT *
FROM layoffs_staging2;


-- b. STANDARDIZING DATA
-- is finding issue in our data and we fixing it

-- kita bakal nyamain data biar ngga ada data yang seharusnya satu kategori tapi malah berbeda atau membuat kategori sendiri
-- pertama kita liat di list company nya, ternyata ada company yang diawali dengan spasi (beda dari yang lain)
SELECT *
FROM layoffs_staging2;
SELECT company, TRIM(company)
FROM layoffs_staging2;
UPDATE layoffs_staging2
SET company = TRIM(company);
-- abis tu kita cek ke kolom yang lain contoh kolom industry, ada kemungkinan industri yang sama namun beda pencatatan seolah olah ada dua jenis industri
SELECT DISTINCT industry 
FROM layoffs_staging2
ORDER BY 1 ;
SELECT * 
FROM layoffs_staging2
WHERE industry LIKE 'crypto%';
-- nah ini kita ketemu crypto punya 3 jenis industri sendiri padahal harus nya cuma 1, langsung kita ubah jadi satu indsutri aja gaes
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'; 
-- jangan lupa cek lagi guys
SELECT DISTINCT industry 
FROM layoffs_staging2
ORDER BY 1 ;

-- kita coba cek kolom lain sekarang locate
SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;
-- aman ternyata cuyyy

-- kita coba cek kolom 'country'
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;
-- ternyata ada masalah guys di usa
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country like 'United States%';
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;
-- anjay dah beres

-- cek dan Benerin definisi data
-- disini kita bakaln cek data yang berkaitan dengan waktu (disini DATE) ini penting banget apalagi kalo kita butuh data time series
SELECT 
    `date`
FROM
    layoffs_staging2; 
-- disini definisi data nya masih berupa text dan format data nya masih berantakan, kita perbaiki format datanya dulu
SELECT `date`,
	STR_TO_DATE(`date`, '%m/%d/%Y')
FROM
    layoffs_staging2;
-- yak aman, selanjutnya kita update
UPDATE layoffs_staging2
SET `date`= STR_TO_DATE(`date`, '%m/%d/%Y');
-- kalo diperhaiiin tinggal definisi data nya aja ni yang masih 'text'
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;
-- yes kelar, note cara ini nggaboleh diterapin di data raw, only for staging

-- c. NULL AND BLANK VALUES
-- kita check untuk null value ada di column industry, total laid off, percentage laid off, dan funds raised millions
SELECT *
FROM layoffs_staging2;
-- setelah itu kita coba fokus ke null di total dan percentage laid off, jika ada row yang keduanya null maka row itu dianggap tidak penting 
-- row tidak penting = bisa kita hapus nanti
SELECT *
FROM layoffs_staging2
WHERE total_laid_off is NULL
AND percentage_laid_off is NULL;
-- abis itu kita coba fokus ke industry, karena ada kemungkinan kesalahan input data sehingga ada null dan blank values 
SELECT *
FROM layoffs_staging2
WHERE industry is NULL
OR industry = '';
-- yak ketemu ada 4 data jenis industri company yang blank/null next kita periksa atu-satu ada ngga data company serupa yang industry nya jelas
-- Airbnb
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';
SELECT *
FROM layoffs_staging2
WHERE company = `Bally's Interactive`;
-- cari semua
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
WHERE (t1.industry is NULL OR t1.industry ='')
AND t2.industry is NOT NULL;
SELECT t1.company,t2.company,t1.industry,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
WHERE (t1.industry is NULL OR t1.industry ='')
AND t2.industry is NOT NULL;
-- yak kita nemu isi data yang seharusnya diisi di row yang blank atau null 
-- NEXT kita ganti semua Blank VALUE ke NULL VALUE biar gampang ngisi row secara otomatis ngga satu2
UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';
-- setelah jadi null semua kita update null ke data yang seharusnya
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry is NULL 
AND t2.industry is NOT NULL;
-- yak udah kelar di update bisa cek lagi
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- sekarang kita coba fokus ke tiga kolumn sisanya dan jujur untuk null dan blank value disini kita ngga bisa isi, karena keterbatasan data, selanjutnya yang akan kita lakukan adalah menghapus data yang sudah pasti tidak penting. yaitu data yang total dan percentage laid off nya blank atau NULL. data itu jelas ngga bakal kepake
SELECT *
FROM layoffs_staging2
WHERE total_laid_off is NULL
AND percentage_laid_off is NULL;
DELETE 
FROM layoffs_staging2
WHERE total_laid_off is NULL
AND percentage_laid_off is NULL;
-- yak udah kehapus
-- final nya kita hapus kolom row_num karena emang data udah bersih dan row_num ngga kita pake lagi
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
SELECT *
FROM layoffs_staging2
-- dan finally our data cleaning is doneee