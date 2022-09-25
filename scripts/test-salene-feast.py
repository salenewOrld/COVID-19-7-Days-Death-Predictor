from feature_store import FeatureStore, Entity, Feature
import pandas as pd
import sys
def test():
    feast = FeatureStore()
    entity = Entity("only_sf")
    df = pd.read_csv('/usr/src/etled-data/etled-data.csv')
    for j in df.columns:
        if str(j[len(j) - 1]) + str(j[len(j) - 2]) == 'fS':
            print(f'Feature {j}')
            new_features = Feature(j, df[j])
            entity.add_entity_features(new_features)
            print(entity.features)
    feast.save(entity, 'Migraine-Classification')
if __name__ == '__main__':
    test()