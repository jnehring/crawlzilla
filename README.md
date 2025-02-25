SELECT url, url_host_registered_domain, content_languages
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2024-51'
  AND subset = 'warc'
  AND content_languages IN ('swa', 'kin', 'yor', 'run', 'hau', 'amh', 'orm', 'lin');
