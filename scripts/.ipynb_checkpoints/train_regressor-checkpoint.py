from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.linear_model import LinearRegression, SGDRegressor, BayesianRidge, ElasticNet
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
from sklearn.preprocessing import StandardScaler
from sklearn import preprocessing
from sklearn.preprocessing import Binarizer
from sklearn.preprocessing import Normalizer
from sklearn.model_selection import train_test_split
import pandas as pd
import yaml
import sys
from etl import *
import mlflow
import pprint
import json
import joblib
#sys.path.insert(0, '/usr/src/')
class Train:
    def __init__(self, config):
        self.config = config
        self.experiment_name = self.config['experiment_name']
    def load_models(self, data: dict):
        models = list()
        for k, v in self.config['models'].items():
            #print(v)
            for parent_lib, model_type in v.items():
                for j in model_type:
                    models.append(eval(f'{j}()'))
        return models
    def eval_metrics(self, y_true, y_pred, scaler_y):
        if self.config['is_scaled']['y'] == 1:
            print('Scaling data in Evaluation')
            y_true_new = scaler_y.inverse_transform(y_true.reshape(-1, 1))
            y_pred_new = scaler_y.inverse_transform(y_pred.reshape(-1, 1))
            y_true = y_true_new
            y_pred = y_pred_new
        r2 = r2_score(y_true, y_pred)
        mae = mean_absolute_error(y_true, y_pred)
        mse = mean_squared_error(y_true, y_pred)
        rmse = mean_squared_error(y_true, y_pred, squared=False)
        return [r2, mae, mse, rmse]
    def train(self, models, x_train, x_test, y_train, y_test, scaler_y):
        try :
            current_experiment = dict(mlflow.get_experiment_by_name(self.experiment_name))
            ex_id = current_experiment['experiment_id']
        except:
            ex_id = mlflow.create_experiment(self.experiment_name)
        #labels = self.config['datasets']['labels']
        print(models)
        for j in models:
            #mlflow.set_tracking_uri("file:///usr/src/mlruns")
            with mlflow.start_run(experiment_id=ex_id):
                j.fit(x_train, y_train)
                metrics = self.eval_metrics(y_test, j.predict(x_test), scaler_y)
                metrics_artifact = {
                    'r2' : metrics[0],
                    'mae' : metrics[1],
                    'mse' : metrics[2],
                    'rmse' : metrics[3]
                }
                #metrics = metrics.to_dict()
                #mlflow.log_param(f'{str(j)}', 1)
                mlflow.log_metric('r2', metrics[0])
                mlflow.log_metric('mae', metrics[1])
                mlflow.log_metric('mse', metrics[2])
                mlflow.log_metric('rmse', metrics[3])
                mlflow.sklearn.log_model(j, str(j))
                with open('metrics.json', 'w') as metrics_file:
                    metrics_file.write(json.dumps(metrics_artifact, indent=4))
                mlflow.log_artifact('metrics.json', str(j))
                mlflow.end_run()
    def split_data(self):
        
        x = pd.read_csv(self.config['datasets']['path'])
        y = pd.read_csv(self.config['datasets']['path_y'])
        print(y.columns)
        rows_test = int(self.config['datasets']['train_test_split']['test_size']) * x.shape[0]
        x_train = x.loc[(x.index >= 0) & (x.index <= x.shape[0] - (rows_test - 1))]
        x_train = x_train[self.config['datasets']['x']]
        y_train = y.loc[(y.index >= 0) & (y.index <= y.shape[0] - (rows_test - 1))]
        y_train = y_train[self.config['datasets']['y']].to_numpy().reshape(-1, 1)
        x_test = x.loc[x.index >= rows_test]
        x_test = x_test[self.config['datasets']['x']]
        y_test = y.loc[y.index >= rows_test]
        y_test = y_test[self.config['datasets']['y']].to_numpy().reshape(-1, 1)
        scaler_y = StandardScaler()
        if self.config['is_scaled']['x'] == 1:
            print("Scaled X")
            if self.config['is_scaled']['scaler'] == 'StandardScaler':
                scaler_x = StandardScaler()
                x_train = scaler_x.fit_transform(x_train)
                x_test = scaler_x.transform(x_test)
            elif self.config['is_scaled']['scaler'] == 'MinMaxScaler':
                scaler_x = preprocessing.MixMaxScaler()
                x_train = scaler_x.fit_transform(x_train)
                x_test = scaler_x.transform(x_test)
            elif self.config['is_scaled']['scaler'] == 'Binarizer':
                scaler_x = Binarizer().fit(x_train)
                x_train = scaler_x.fit_transform(x_train)
                x_test = scaler_x.transform(x_test)
            else :
                scaler_x = Normalizer().fit(x_train)
                x_train = scaler_x.fit_transform(x_train)
                x_test = scaler_x.transform(x_test)
            joblib.dump(scaler_x, f'mlruns/scaler_x_{self.experiment_name}.pkl')
        else :
            scaler_x = None
        if self.config['is_scaled']['y'] == 1:
            print("Scaled Y")
            y_train = scaler_y.fit_transform(y_train)
            y_test = scaler_y.transform(y_test)
            joblib.dump(scaler_y, f'mlruns/scaler_y_{self.experiment_name}.pkl')
        else :
            scaler_y = None
        return x_train, x_test, y_train, y_test, scaler_x, scaler_y
    def fit(self):
        models = self.load_models(self.config)
        x_train, x_test, y_train, y_test, scaler_x, scaler_y = self.split_data()
        self.train(models, x_train, x_test, y_train.reshape(-1, 1), y_test.reshape(-1, 1), scaler_y)
        print('Successfully trained')
def read_yaml(FILE_NAME):
    with open(f'/usr/src/configs/{FILE_NAME}', 'r') as yaml_file:
        cfg = yaml.load(yaml_file, Loader=yaml.FullLoader)
    return cfg
if __name__ == '__main__':
    try:
        file_name = sys.argv[1]
    except:
        raise ValueError('Missing necessary variable\n-EXPERIMENT_ID')
    
    cfg = read_yaml(file_name)
    trainer = Train(cfg)
    trainer.fit()