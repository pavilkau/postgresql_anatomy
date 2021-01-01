import pandas as pd

file_name = 'BankChurners.csv'

df = pd.read_csv(file_name, index_col=False)
columns = ["Customer_Age","Gender","Dependent_count","Education_Level","Marital_Status","Income_Category"]

df.to_csv('output.csv', columns = columns, index=False)


# create table bank_churners(id serial, age integer, gender text, dependent_count integer, education text, marital_status text, income_category text);
# copy bank_churners(age, gender, dependent_count, education, marital_status, income_category) from '/home/chridcrow/anatomy_gdpr_ba/output.csv' delimiter ',' csv header;
