import pandas as pd

data = {'Name': ['Alice', 'Bob', 'Charlie'],
        'Age': [25.0, 30, 35]}

df = pd.DataFrame(data)
print(df.info())