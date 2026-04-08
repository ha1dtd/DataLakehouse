import pandas as pd

df = pd.read_parquet("part-00000-f6863d4c-8589-4671-a389-b6b95eb06f5e-c000.snappy.parquet")
print(df)