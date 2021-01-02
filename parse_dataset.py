import pandas as pd

file_name = 'BankChurners.csv'

df = pd.read_csv(file_name, index_col=False)
columns = ["Customer_Age","Gender","Dependent_count","Education_Level","Marital_Status","Income_Category"]

df.to_csv('output.csv', columns = columns, index=False)
