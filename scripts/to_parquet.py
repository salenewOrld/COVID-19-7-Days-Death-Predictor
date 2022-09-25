import pandas as pd
import sys
#sys.path.insert(0, 'usr/src/etled-data')
df = pd.read_csv('/usr/src/etled-data/etled-data.csv').to_parquet('main_data.parquet')
