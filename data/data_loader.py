import pandas as pd

def load_data(file_path):
    data = pd.read_csv(file_path, encoding='cp949')
    return data