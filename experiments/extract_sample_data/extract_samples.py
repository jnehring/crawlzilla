import json
import gzip
import os
import shutil
import numpy as np
import pandas as pd

infolder = '../../outputs/'

df = []

for lang in os.listdir(infolder):
    print(lang)
    text_folder = os.path.join(infolder, lang, 'textual_outputs')
    if not os.path.exists(text_folder):
        continue

    files = sorted(os.listdir(text_folder))
    if len(files) < 5:
        break

    lines = []
    
    for file in files[0:3]:
        for line in open(os.path.join(text_folder, file)):
            lines.append(line)

    indizes = np.random.choice(np.arange(len(lines)), size=100)
    for i in indizes:
        df.append([lines[i], lang])

df = pd.DataFrame(df, columns=['sample', 'language'])
df.to_csv('samples.csv')

print(df.language.value_counts())