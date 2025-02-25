Commoncrawl Language Codes: https://en.wikipedia.org/wiki/ISO_639:b

SQL query to download all Kinyarwanda websites

SELECT COUNT(*) AS count,
       url_host_registered_domain
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2024-51'
  AND subset = 'warc'
  AND content_languages = 'kin'
GROUP BY  url_host_registered_domain
HAVING (COUNT(*) >= 1)
ORDER BY  count DESC


Explanation of common crawl data format

https://skeptric.com/common-crawl-index-athena/






SELECT sub1.count_kin, sub1.domain, sub2.count_total
FROM 
(SELECT COUNT(*) AS count_kin,
       url_host_registered_domain as domain
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2024-51'
  AND subset = 'warc'
  AND content_languages = 'kin'
GROUP BY  url_host_registered_domain
HAVING (COUNT(*) >= 100)
ORDER BY  count DESC) AS sub 1,
(SELECT COUNT(*) AS count_total, url_host_registered_domain as domain
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2024-51'
  AND subset = 'warc'
  GROUP BY  url_host_registered_domain) as sub2
WHERE sub1.domain == sub2.domain
