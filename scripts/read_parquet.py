import pandas as pd

parquet = pd.read_parquet(path="parquet/part-00000-2fa892d7-e286-4a6e-abf9-2edec1f19b84-c000.snappy.parquet",
                          engine='pyarrow')

print(parquet.head())