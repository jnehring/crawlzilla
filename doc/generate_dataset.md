# How to generate a Parquet dataset from the individual crawls.

This document explains how to generate the final dataset.

You can use the `generate_dataset.py` script to generate the final data files. The script works as follows:

1. It iterates over all files in the folder `textual_outputs` for this languages. By default, its assumes that the outputs are located in `../outputs`.
2. For each language, it maintains a hash set for each sentence to perform sentence deduplication.
3. It writes its final outputs and some statistics to `../outputs/final_output`.

Here is the help section of the `generate_dataset.py` script:

```
$ python generate_dataset.py -h
usage: Crawlzilla Dataset Generator [-h] [--input_folder INPUT_FOLDER]
                                    [--output_folder OUTPUT_FOLDER] [--languages LANGUAGES]
                                    [--batch_size BATCH_SIZE]

Generate a Parquet dataset from all crawls.

options:
  -h, --help            show this help message and exit
  --input_folder INPUT_FOLDER
                        folder in which the input data is stored. by default, this is
                        ../outputs
  --output_folder OUTPUT_FOLDER
                        Folder in which the output data is stored. by default, this is
                        ../outputs/final_dataset
  --languages LANGUAGES
                        Limit to certain languages. You can comma-separate the languages,
                        e.g., kin_Latn,hau_Arab
  --batch_size BATCH_SIZE
                        Number of samples in each batch. Default is 500.000 which writes
                        batches of about 40MB to disk.

```

## Example usage

Here is an example call of the script

```
python generate_dataset.py
```

The script generates the parquet dataset in `../outputs/final_output/dataset`. It generates statistics (counts words, characters and sentences), prints them to the console and safes them as CSV to `../outputs/final_dataset/stats.csv`. 
```