-- EXPLORATORY DATA ANALYSIS
-- We kind of discover and go about things as we are learning and looking at the data set
-- kita coba cari insight dari data yang udah kita bersihin
SELECT *
FROM layoffs_staging2;

-- coba kita liat ke topik utama (laid off) kita bakal cari tahu angka terbesar(maksimum) yang pernah dicapai dari total dan persentase laid off nya
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

-- menariknya ada perusahaan yang terkena laid off sebesar 1 atau 100%, kita coba cari tau perusahaan apa aja si 
-- tentunya kita juga mengurutkan dari total terbanyak
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;
-- kita coba urutkan berdasarkan dari funds raise
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- hmm kita cari tahu perusahaan dengan total laid off terbanyak
SELECT company,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;
-- amazon, menjadi perusahaan dengan total laid off terbanyak, disusul google dan meta

-- next kita check jangka waktu datanya dari data terlawas hingga terkini
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;
-- yak periode data ada di masa pandemi COVID-19 sampai masa pemulihan pandemi covid-19, disini tingkat ketidakpastian ekonomi masih tinggi

-- setelah company, kemudian kita cari industry mana yang paling terdampak
SELECT industry,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;
-- nampaknya industri konsumsi dan ritel yang terkena dampak paling besar dibanding industri lain

-- next kita cari negara dengan laid off terbanyak
SELECT country,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;
-- yap US dengan total laid off yang sangat signifikan, bahkan kalo dibandingin sama india yang menempati posisi 2 itu ngga ada apa-apanya 

-- next kita cari tau jumlah laid off per tahun
SELECT YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY  YEAR(`date`)
ORDER BY 1 DESC;
-- 2022 is the worst year

-- next kita cari berdasarkan company stage, kira-kira perusahaan tingkat apa yang paling banyak terdampak
 SELECT stage,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY  stage
ORDER BY 2 DESC;
-- yak post-IPO company
SELECT company, stage,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY  company, stage
ORDER BY 3 DESC;
-- dan ini rincian lebih detailnya

-- next kita mau coba bikin rolling sum buat liat progression lay off nya dari awal sampai data terakhir
SELECT SUBSTRING(`date`, 1, 7 ) AS `MONTH`,SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7 ) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;
-- diatas ada total laid off per bulan dari bulan maret 2020
-- kemudian kita bikin rolling sum pake CTE
WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1, 7 ) AS `MONTH`,SUM(total_laid_off) AS layoff
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7 ) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
) 
SELECT `MONTH`, layoff , SUM(layoff) 
OVER (ORDER BY `MONTH`)AS Rolling_Total
FROM Rolling_Total;
-- dari rolling total atau Rolling SUM ini kita bisa liat progress total layoff seiring bergantinya bulan 

-- NEXT AND PROBABLY LAST kita bakalan cari tau peringkat company dengan total laid off terbanyak pada tiap tahun
-- kita bikin dasar query atau susunan data nya dulu biar ngga pusing dengan susunan (COMPANY,YEAR,TOTAL LAID OFF)
SELECT company,YEAR(`DATE`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,YEAR(`DATE`)
ORDER BY 3 DESC; 
-- lalu kita kembangkan lagi jadi (COMPANY, YEAR, TOTAL LAID OFF, RANKING) pake CTE lagi
WITH Company_Year (Company, Years, Total_Laid_Off ) AS 
	(
	SELECT company,YEAR(`DATE`), SUM(total_laid_off)
	FROM layoffs_staging2
	GROUP BY company,YEAR(`DATE`)
	)
SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_Off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
ORDER BY Ranking ASC;
-- kita bisa liat rank 1 tiap tahunnya
-- next kita bikin yang lebih ringkas dan rapi pake 2 CTE, sekarang kita mau bikin list TOP 5 Laid Off Company di tiap tahun
WITH Company_Year (Company, Years, Total_Laid_Off ) AS 
	(
	SELECT company,YEAR(`DATE`), SUM(total_laid_off)
	FROM layoffs_staging2
	GROUP BY company,YEAR(`DATE`)
	),
Company_Years_Rank AS
	(
	SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_Off DESC) AS Ranking
	FROM Company_Year
	WHERE years IS NOT NULL
	)
SELECT *
FROM Company_Years_Rank
WHERE Ranking <= 5;
-- dan jadilah TOP 5 Company dengan total Laid off terbanyak tiap tahun nya 