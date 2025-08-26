-- ======================================================
-- ALL-IN-ONE ATHENA SQL WORKBOOK for tbl_datalake
-- ======================================================

-- ======================================================
-- SECTION 1: BASIC DATA EXPLORATION
-- ======================================================

-- 1. Preview first 10 records
SELECT *
FROM tbl_datalake
LIMIT 10;

-- 2. Select specific columns
SELECT track_name, artist_name, album_name, genre, duration_sec
FROM tbl_datalake
LIMIT 10;

-- 3. Filter by genre and track popularity
SELECT track_name, artist_name, track_popularity, genre
FROM tbl_datalake
WHERE genre = 'hardcore'
  AND track_popularity > 10
ORDER BY track_popularity DESC
LIMIT 20;

-- 4. Average track popularity by genre
SELECT genre,
       AVG(track_popularity) AS avg_popularity,
       COUNT(*) AS total_tracks
FROM tbl_datalake
GROUP BY genre
ORDER BY avg_popularity DESC;

-- 5. Top 10 artists by total followers
SELECT artist_name, SUM(followers) AS total_followers
FROM tbl_datalake
GROUP BY artist_name
ORDER BY total_followers DESC
LIMIT 10;

-- 6. Tracks released in a specific year (2023)
SELECT track_name, artist_name, release_date
FROM tbl_datalake
WHERE year(release_date) = 2023
ORDER BY release_date DESC;

-- 7. Top 10 longest tracks
SELECT track_name, artist_name, duration_sec
FROM tbl_datalake
ORDER BY duration_sec DESC
LIMIT 10;

-- 8. Album-level aggregation
SELECT album_name, album_id,
       COUNT(track_id) AS total_tracks,
       AVG(track_popularity) AS avg_track_popularity,
       MAX(track_popularity) AS most_popular_track
FROM tbl_datalake
GROUP BY album_name, album_id
ORDER BY avg_track_popularity DESC;

-- 9. Track popularity vs artist followers
SELECT track_name, artist_name, track_popularity, followers
FROM tbl_datalake
WHERE followers IS NOT NULL
ORDER BY followers DESC
LIMIT 20;

-- 10. Filter using release_date and genre (partition efficiency)
SELECT track_name, artist_name, genre
FROM tbl_datalake
WHERE release_date BETWEEN DATE '2023-01-01' AND DATE '2023-12-31'
  AND genre = 'karoke';

-- ======================================================
-- SECTION 2: ADVANCED ANALYTICS
-- ======================================================

-- 11. Top 5 tracks per genre by popularity
WITH genre_ranked AS (
    SELECT track_name, artist_name, genre, track_popularity,
           ROW_NUMBER() OVER (PARTITION BY genre ORDER BY track_popularity DESC) AS rank
    FROM tbl_datalake
)
SELECT *
FROM genre_ranked
WHERE rank <= 5
ORDER BY genre, rank;

-- 12. Top 10 artists by average track popularity
SELECT artist_name,
       AVG(track_popularity) AS avg_track_popularity,
       COUNT(track_id) AS total_tracks
FROM tbl_datalake
GROUP BY artist_name
ORDER BY avg_track_popularity DESC
LIMIT 10;

-- 13. Albums with highest average track popularity
SELECT album_name, album_id,
       AVG(track_popularity) AS avg_track_popularity,
       COUNT(track_id) AS total_tracks
FROM tbl_datalake
GROUP BY album_name, album_id
HAVING COUNT(track_id) > 1
ORDER BY avg_track_popularity DESC
LIMIT 10;

-- 14. Genre-wise total duration and average track duration
SELECT genre,
       SUM(duration_sec)/3600 AS total_duration_hours,
       AVG(duration_sec) AS avg_track_duration_sec,
       COUNT(track_id) AS total_tracks
FROM tbl_datalake
GROUP BY genre
ORDER BY total_duration_hours DESC;

-- 15. Artists with most followers per genre
WITH artist_genre_followers AS (
    SELECT artist_name, genre, SUM(followers) AS total_followers
    FROM tbl_datalake
    GROUP BY artist_name, genre
)
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY genre ORDER BY total_followers DESC) AS rank
    FROM artist_genre_followers
) t
WHERE rank = 1
ORDER BY genre;

-- 16. Correlation between track popularity and artist popularity
SELECT artist_name, 
       AVG(track_popularity) AS avg_track_popularity,
       AVG(artist_popularity) AS avg_artist_popularity,
       SUM(followers) AS total_followers
FROM tbl_datalake
GROUP BY artist_name
ORDER BY avg_track_popularity DESC
LIMIT 20;

-- 17. Most productive years (tracks released per year)
SELECT year(release_date) AS release_year,
       COUNT(track_id) AS total_tracks
FROM tbl_datalake
GROUP BY year(release_date)
ORDER BY total_tracks DESC;

-- 18. Average track popularity by release year and genre
SELECT year(release_date) AS release_year, genre,
       AVG(track_popularity) AS avg_track_popularity,
       COUNT(track_id) AS total_tracks
FROM tbl_datalake
GROUP BY year(release_date), genre
ORDER BY release_year DESC, avg_track_popularity DESC;

-- 19. Tracks longer than 10 minutes (600 sec) with high popularity (>80)
SELECT track_name, artist_name, duration_sec, track_popularity
FROM tbl_datalake
WHERE duration_sec > 600
  AND track_popularity > 80
ORDER BY track_popularity DESC;

-- 20. Label-wise aggregation (albums and average album popularity)
SELECT label,
       COUNT(DISTINCT album_id) AS total_albums,
       AVG(album_popularity) AS avg_album_popularity,
       COUNT(track_id) AS total_tracks
FROM tbl_datalake
GROUP BY label
ORDER BY avg_album_popularity DESC;

-- ======================================================
-- SECTION 3: CUSTOM FILTERING & EFFICIENCY TIPS
-- ======================================================

-- Example: Popular classic tracks in 2022
SELECT track_name, artist_name, track_popularity, release_date
FROM tbl_datalake
WHERE genre = 'classic'
  AND track_popularity > 75
  AND release_date BETWEEN DATE '2022-01-01' AND DATE '2022-12-31'
ORDER BY track_popularity DESC
LIMIT 20;

-- Example: Artists with high popularity and many followers
SELECT artist_name, AVG(artist_popularity) AS avg_artist_popularity, SUM(followers) AS total_followers
FROM tbl_datalake
GROUP BY artist_name
HAVING SUM(followers) > 1000000
ORDER BY avg_artist_popularity DESC
LIMIT 20;
