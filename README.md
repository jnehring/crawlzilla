srun -K  \
  --container-mounts=/data:/data,$HOME:$HOME \
  --container-workdir=$PWD \
  --container-image=/data/enroot/nvcr.io_nvidia_pytorch_22.05-py3.sqsh \
  --mem 16GB \
  --pty \
  /bin/bash



srun -K  \
  --container-mounts=/data:/data,$HOME:$HOME \
  --container-workdir=$PWD \
  --container-image=/data/enroot/nvcr.io_nvidia_pytorch_22.05-py3.sqsh \
  --mem 16GB \
  start_crawler.sh lin_Latn


SELECT url, url_host_registered_domain, content_languages
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2024-51'
  AND subset = 'warc'
  AND content_languages IN ('swa', 'kin', 'yor', 'run', 'hau', 'amh', 'orm', 'lin');
