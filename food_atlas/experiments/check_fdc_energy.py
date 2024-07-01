import pandas as pd


ff_food = pd.read_csv(
    "data/FDC/FoodData_Central_foundation_food_csv_2024-04-18/foundation_food.csv"
)
data = pd.read_csv(
    "data/FDC/FoodData_Central_foundation_food_csv_2024-04-18/food_nutrient.csv"
)
data = data[data["fdc_id"].isin(ff_food["fdc_id"])]
foods = set(data['fdc_id'].tolist())

data = data[data['nutrient_id'].isin([2047, 2048, 1008, 1062])]
foods_energy = set(data['fdc_id'].tolist())

print(foods - foods_energy)
