SELECT content_languages, COUNT(*) AS count
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2024-51'
GROUP BY  content_languages
ORDER BY  count DESC


https://www.languagecourse.net/languages-worldwide