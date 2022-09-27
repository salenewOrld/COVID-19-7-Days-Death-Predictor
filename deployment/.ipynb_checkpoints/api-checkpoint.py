from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import pandas as pd
import sys
import yaml
class Features(BaseModel):
    total_cases: float
    new_cases: int
    total_deaths: int
    reproduction_rate: float
    total_test: float
    new_tests: float
    positive_rate: float
    tests_per_case: int
    tests_units: int
    total_vaccinations: int
    people_vaccinated: int
    people_full_vaccinated: int
    total_boosters: int
    new_vaccinations: int
    stringency_index: float
    population: int
    population_density: float
    median_age: int
    aged_65_older: float
    aged_70_older: float
    gdp_per_capita: float
    extreme_poverty: float
    cardiovasc_death_rate: float
    diabetes_prevalence: float
    female_smokers : float
    male_smokers: float
    handwashing_facilities: float
    hospital_beds_per_thousand: float = 2.1
    life_expectancy: float
    human_development_index: float
    year : int
    month : int
    day : int



app = FastAPI()
return_body = {
    "Response" : {}
}
@app.get("/")
async def root():
    body = return_body.copy()
    body['Response']['Message'] = "Test"
    return body

@app.post("/get_new_death")
async def get_new_death(features: Features):
    sys.path.insert(0, '/Users/salene/Documents/GitHub/covid19-new-deaths-prediction/')
    with open('rf-deploy.yaml', 'r') as yaml_file:
        config = yaml.load(yaml_file, Loader=yaml.FullLoader)
    model = joblib.load(f'/Users/salene/Documents/GitHub/covid19-new-deaths-prediction/scripts/mlruns/{config["experiment_id"]}/{config["run_id"]}/artifacts/{config["algorithm_name"]}/model.pkl')
    #if config['scale']['is_scaled'] == 1:
    if config['scale']['has_scaler']['x'] == 1:
        scaler_x = joblib.load(f'/Users/salene/Documents/GitHub/covid19-new-deaths-prediction/scripts/mlruns/{config["scale"]["scaler_x"]}')
    if config['scale']['has_scaler']['y'] == 1:
        scaler_y = joblib.load(f'/Users/salene/Documents/GitHub/covid19-new-deaths-prediction/scripts/mlruns/{config["scale"]["scaler_y"]}')
    dict = {
        "total_cases": [features.total_cases],
    "new_cases": [features.new_cases],
    "total_deaths": [features.total_deaths],
    "reproduction_rate": [features.reproduction_rate],
    "total_tests": [features.total_test],
    "new_tests": [features.new_tests],
    "positive_rate": [features.positive_rate],
    "tests_per_case": [features.tests_per_case],
    "tests_units": [features.tests_units],
    "total_vaccinations": [features.total_vaccinations],
    "people_vaccinated": [features.people_vaccinated],
    "people_fully_vaccinated": [features.people_full_vaccinated],
    "total_boosters": [features.total_boosters],
    "new_vaccinations": [features.new_vaccinations],
    "stringency_index": [features.stringency_index],
    "population": [features.population],
    "population_density": [features.population_density],
    "median_age": [features.median_age],
    "aged_65_older": [features.aged_65_older],
    "aged_70_older": [features.aged_70_older],
    "gdp_per_capita": [features.gdp_per_capita],
    "extreme_poverty": [features.extreme_poverty],
    "cardiovasc_death_rate": [features.cardiovasc_death_rate],
    "diabetes_prevalence": [features.diabetes_prevalence],
    "female_smokers": [features.female_smokers],
    "male_smokers": [features.male_smokers],
    "handwashing_facilities": [features.handwashing_facilities],
    "hospital_beds_per_thousand": [features.hospital_beds_per_thousand],
    "life_expectancy": [features.life_expectancy],
    "human_development_index": [features.human_development_index],
    "year": [features.year],
    "month": [features.month],
    "day": [features.day]
    }
    df = pd.DataFrame(dict)
    #scaler_x = joblib.load(f'/Users/salene/Documents/GitHub/covid19-new-deaths-prediction/scripts/mlruns/{config["scaler_x"]}')
    #scaler_y = joblib.load(f'/Users/salene/Documents/GitHub/covid19-new-deaths-prediction/scripts/mlruns/{config["scaler_y"]}')
    #print(df.columns)
    x_test = df[df.columns]
    #print(x_test)
    if config['scale']['has_scaler']['x'] == 1:
        x = scaler_x.transform(x_test)
        x_test = x
    result = model.predict(x_test)
    result = result.reshape(-1, 1)
    if config['scale']['has_scaler']['y'] == 1:
        inversed = scaler_y.inverse_transform(result)
        result = inversed
    return {"Response" : {"NewDeathPredicted" : int(result[0])}}
