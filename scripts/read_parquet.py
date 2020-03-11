import pandas as pd
import argparse

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", type=str)
    return parser.parse_args()

args = parse_args()

parquet = pd.read_parquet(path=args.filename,
                          engine='pyarrow')

print(parquet.head())