import pandas as pd
import sqlite3
from utils.yaml_parser import get_tables

# create a connection to the SQLite database
conn = sqlite3.connect('test.db')

# create a pandas DataFrame
# df = pd.DataFrame({'id': [1, 2, 3], 'val': ['a', 'b', 'c']})

#df = pd.DataFrame({'id': [4,5], 'val': ['d', 'e']})


# write the DataFrame to an existing SQLite table
#df.to_sql(name='my_table', con=conn, if_exists='append', index=False)

cursor = conn.cursor()
#cursor.execute("create table my_table ( id int, val string)")

cursor.execute("Drop table client_dim")
# tables = get_tables()


# with open("sql/ddl_orders_fact.sql", 'r') as f:
#                 create_query = f.read()

# cursor.executescript(create_query)
# close the database connection



def testing():
    table_details = tables['orders_fact']
    file_columns = []
    table_mapped_columns = [] 
    types = []
    validations = []
    
    for each_column_detail in table_details:
        file_columns.append(each_column_detail.get('file_column'))
        table_mapped_columns.append(each_column_detail.get('table_mapped_column'))
        types.append(each_column_detail.get('type'))
        validations.append(each_column_detail.get('validations'))
    print(file_columns)
    print(table_mapped_columns)
    print(types)    

    df = pd.read_csv("data/data.csv")
    required_df = df[file_columns]
    required_df = required_df.rename(columns=dict(zip(file_columns,table_mapped_columns)))
    print(required_df.head())

    required_df.to_sql(name='orders_fact', con=conn, if_exists='append', index=False)


# testing()    

def pds():
    # create sample data
    data = {'firstname': ['John', 'John', 'Alex', 'Alex' ],
            'lastname': ['Doe', 'Doe', 'Smith', 'Smith']}
            #'date': ['2021-01-01', '2021-01-01',  '2021-01-01', '2021-01-01']}
    df = pd.DataFrame(data)
    df['date'] = pd.Timestamp.max
    df['date'] = pd.to_datetime(df['date']).dt.date

    # sort the dataframe by firstname, lastname, and date in descending order
    #df.sort_values(['firstname', 'lastname', 'date'], ascending=[True, True, False], inplace=True)

    # add a new column to the dataframe to store the rank for each name
    # df['rank'] = df.groupby(['firstname', 'lastname'])['date'].rank(method='first', ascending=False)

    # # filter the dataframe to keep only the records where rank = 1
    # df_latest = df[df['rank'] == 1]

    # # drop the rank column from the dataframe
    # df_latest = df_latest.drop(columns=['rank'])

    # display the result
    print(df)

#pds()