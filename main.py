# ETL Pipeline Project
import pandas as pd
import pandas as pd
from sqlalchemy import create_engine
# Extracting data
data=pd.read_csv('finance.csv')
print(data.head()) #testing data is extracted or not
#Transformation DATA
# enriching data
# Split the 'info' column into 'Title' and 'Description', handling missing delimiters
split_columns = data['info'].str.split(':',n=1, expand=True)
data['Title'] = split_columns[0].str.strip()
data['Description'] = split_columns[1].str.strip() if len(split_columns.columns) > 1 else ''

# Split the 'created' column into 'Date_created' and 'Time_created', handling missing delimiters
split_colums=data['created'].str.split('T',n=1,expand=True)
data['Date_created']=split_colums[0].str.strip()
data['Time_created']=split_colums[1].str.strip() if len(split_colums.columns) > 1 else ''

# Split the 'published_time' column into 'Date_published' and 'Time_published', handling missing delimiters
split_colums=data['published_time'].str.split('T',n=1,expand=True)
data['Date_published']=split_colums[0].str.strip()
data['Time_published']=split_colums[1].str.strip() if len(split_colums.columns) > 1 else ''

#dropping un necesary columns
data = data.drop(columns=['info'])
data = data.drop(columns=['published_time'])
data=data.drop(columns=['price_price_string'])
#Rounding floating data
data['avg_rating']=data['avg_rating'].round(2)
data['avg_recent_rating']=data['avg_recent_rating'].round(2)
data['rating']=data['rating'].round(2)
# Save the updated DataFrame back to a CSV file
data.to_csv('/media/faisal/5A4CFC174CFBEC1F/internship/finance.csv', index=False)
print("CSV file updated successfully.")

# Loading data to Database/warehouse

engine = create_engine('postgresql://your_username:your_password@localhost/databseName')

# Load the DataFrame into the PostgreSQL table
data.to_sql('table_Name', engine, if_exists='replace', index=False)

print("Data loaded successfully.")

