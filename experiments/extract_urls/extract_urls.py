import json
import gzip
import os
import shutil

infolder = '../../outputs/'
outfolder = 'outputs'

if os.path.exists(outfolder):
    shutil.rmtree(outfolder)

os.mkdir(outfolder)

for lang in os.listdir(infolder):
    print(lang)
    parsed_folder = os.path.join(infolder, lang, 'parsed')
    if not os.path.exists(parsed_folder):
        continue

    writer = gzip.open(os.path.join(outfolder, lang + ".txt.gz"), 'wt')

    for parsed_file in os.listdir(parsed_folder):
        parsed_file = os.path.join(parsed_folder, parsed_file)

        try:
            for line in gzip.open(parsed_file, "rb"):
                data = json.loads(line)
                writer.write(data['url'])
                writer.write("\n")
        except Exception as e:
            print(e)
    
    writer.close() 