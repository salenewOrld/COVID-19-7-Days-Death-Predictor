import pymongo
import sys

class FeatureStore:
    def __init__(self) -> None:
        pass
    def save(self, entity, project_name: str):
        client = pymongo.MongoClient("mongodb://root:1234@mongodb:27017/?directConnection=false&serverSelectionTimeoutMS=2000&appName=mongosh+1.5.4")
        #client2 = pymongo.MongoClient("mongodb://root:1234@localhost:27017/feature_store?ssl=true&ssl_cert_reqs=CERT_NONE")
        db = client['feature_store']
        col = db['feature_store']
        query = {
            "Project" : project_name,
            "Entity" : entity.name,
            "Features" : {}
        }
        for j in entity.features:
            query['Features'][j.name] = j.values.copy()
        print(entity.features)
        x = col.insert_one(query)
        
class Entity:
    def __init__(self, entity_name, entity_description=None):
        super().__init__()
        self.name = entity_name
        self.entity_description = entity_description
        self.features = list()
    
    def get_entity_features(self):
        '''
        params: None
        '''
        return self.features
    
    def add_entity_features(self, feature):
        '''
        params: An object called Feature
        '''
        self.features.append(feature)
        return f'Feature {feature.feature_name} has been successfully added to Entity {self.entity_name}'
    
class Feature:
    def __init__(self, name: str, values: list):
        self.name = name
        self.values = values
    def get_feature_values(self):
        return self.values
    
    
    