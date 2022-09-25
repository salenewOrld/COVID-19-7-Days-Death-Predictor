import pymongo

myclient = pymongo.MongoClient("mongodb://root:1234@127.0.0.1/feature_store", 27017)
mydb = myclient["feature_store"]
mycol = mydb["feature_store"]

mydict = { "feature_name": "house_sf", "value_one": "33.88" }

x = mycol.insert_one(mydict)