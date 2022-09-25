import opendatasets as od
import sys

PATH = '/usr/src/datasets'
try: 
    URL = sys.argv[1]
except:
    raise ValueError("The following variable wasn't provided\n-DATASET URL")

od.download(URL, data_dir=PATH)