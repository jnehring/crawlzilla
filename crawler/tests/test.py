from warcio.archiveiterator import ArchiveIterator

with open('tests/assets/temp/static_crawl/warc/00001.warc.gz', 'rb') as stream:
    for record in ArchiveIterator(stream):
        if record.rec_type == 'response':
            print(record.rec_headers.get_header('WARC-Target-URI'))
