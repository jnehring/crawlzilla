Crawl finished for

run_Latn, lin_Latn


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
    --mem 32GB \
    start_crawler.sh lin_Latn


SELECT url, url_host_registered_domain, content_languages
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2024-51'
  AND subset = 'warc'
  AND content_languages IN ('swa', 'kin', 'yor', 'run', 'hau', 'amh', 'orm', 'lin');



SELECT url, url_host_registered_domain, content_languages
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2024-51'
  AND subset = 'warc'
  AND content_languages IN ('swa', 'hau', 'amh', 'yor', 'orm');





  srun -K --container-mounts=/data:/data,$HOME:$HOME --container-workdir=$PWD --container-image=/data/enroot/nvcr.io_nvidia_pytorch_22.05-py3.sqsh --mem 24GB start_crawler.sh kin_Latn



  srun -K  \
    --container-mounts=/data:/data,$HOME:$HOME \
    --container-workdir=$PWD \
    --container-image=/data/enroot/nvcr.io_nvidia_pytorch_22.05-py3.sqsh \
    --mem 24GB \
    start_crawler.sh hau_Latn


  srun -K  \
    --container-mounts=/data:/data,$HOME:$HOME \
    --container-workdir=$PWD \
    --container-image=/data/enroot/nvcr.io_nvidia_pytorch_22.05-py3.sqsh \
    --mem 24GB \
    start_crawler.sh amh_Ethi


  srun -K  \
    --container-mounts=/data:/data,$HOME:$HOME \
    --container-workdir=$PWD \
    --container-image=/data/enroot/nvcr.io_nvidia_pytorch_22.05-py3.sqsh \
    --mem 24GB \
    start_crawler.sh yor_Latn


  srun -K  \
    --container-mounts=/data:/data,$HOME:$HOME \
    --container-workdir=$PWD \
    --container-image=/data/enroot/nvcr.io_nvidia_pytorch_22.05-py3.sqsh \
    --mem 24GB \
    start_crawler.sh orm_Latn





