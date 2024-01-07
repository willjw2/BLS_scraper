'''
William Wang

Python script to scrape Local Area Unemployment Statistics data from https://download.bls.gov/pub/time.series/la/
from urls of the Local Area Unemployment Statistics files and store that data in SQL Server.

NO LONGER FUNCTIONAL
'''

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from io import StringIO
import pyodbc
import sqlalchemy

url_list = ['https://download.bls.gov/pub/time.series/la/la.series', 'https://download.bls.gov/pub/time.series/la/la.data.0.CurrentU00-04',
            'https://download.bls.gov/pub/time.series/la/la.data.0.CurrentU05-09', 'https://download.bls.gov/pub/time.series/la/la.data.0.CurrentU10-14',
            'https://download.bls.gov/pub/time.series/la/la.data.0.CurrentU15-19', 'https://download.bls.gov/pub/time.series/la/la.data.0.CurrentU20-24',
            'https://download.bls.gov/pub/time.series/la/la.data.0.CurrentU90-94', 'https://download.bls.gov/pub/time.series/la/la.data.0.CurrentU95-99',
            'https://download.bls.gov/pub/time.series/la/la.data.1.CurrentS', 'https://download.bls.gov/pub/time.series/la/la.area_type',
            'https://download.bls.gov/pub/time.series/la/la.measure', 'https://download.bls.gov/pub/time.series/la/la.footnote',
            'https://download.bls.gov/pub/time.series/la/la.map_info', 'https://download.bls.gov/pub/time.series/la/la.state_region_division',
            'https://download.bls.gov/pub/time.series/la/la.seasonal', 'https://download.bls.gov/pub/time.series/la/la.period']


# assigning a table name from the url
def getTableName(url):
    tableName = ''
    for character in url[::-1]:
        if character == '.':
            break
        elif character == '-':
            tableName = '_' + tableName
        else:
            tableName = character + tableName
    return tableName


# returning a list of sql data types from dataframe columns
def getColumnDtypes(dataTypes):
    dataList = []
    for x in dataTypes:
        if (x == 'int64'):
            dataList.append('int')
        elif (x == 'float64'):
            dataList.append('float')
        elif (x == 'bool'):
            dataList.append('boolean')
        else:
            dataList.append('varchar (max)')
    return dataList


# connecting to the sequel server using pyodbc, alternatively could maybe use pandas.DataFrame.to_sql
# with pyodbc and sqlalchemy but haven't figured out if that is better or not

conn_str = (
    r"Driver={ODBC Driver 17 for SQL Server};"
    r"Server=testServer;"
    r"Database=TEST;"
    r"Trusted_Connection=yes;"
)

cnxn = pyodbc.connect(conn_str)
engine = sqlalchemy.create_engine(
    "mssql+pyodbc://testServer/TEST?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server")

cursor = cnxn.cursor()

for url in url_list:
    tableName = getTableName(url)

    # use the requests library get function to get the data from the webpage as a response object
    response = requests.get(url)

    # use the beautifulsoup library to take response.content, the webpage's html, and process it
    # to get only the text of the website.
    soup = BeautifulSoup(response.content, 'html.parser')
    text = soup.get_text(strip=True)  # returns only text of the site

    # based on the formatting of the text in the BLS site, format the text to be like a csv file with ; as the delimiter
    test = text.replace('\t', ';')

    # StringIO, from what I understand, converts the string with all of the data to a file object that can be treated
    # just like a file, which lets it be fed easily into a pandas DataFrame
    TESTDATA = StringIO(test)

    # creating a dataframe using the converted data
    # specified the datatypes of each column and converted '-' into NaN (missing values), as the BLS website seems to list some missing values
    # as '-'
    df = pd.read_csv(TESTDATA, sep=";", skipinitialspace=True, na_values='-', engine='c')
    df2 = df.rename(columns=lambda x: x.strip())  # remove leading/trailing spaces from column headers
    df2 = df2.applymap(lambda x: x.strip() if isinstance(x, str) else x)  # remove leading/trailing spaces from columns

    print(df2)  # taking a look at the dataframe with all of the data in it

    columnName = list(df2.columns.values)  # list of column names

    columnDataType = getColumnDtypes(df2.dtypes)  # get list of column datatypes

    # creating a sql statement to create appropriate table if table doesn't already exist
    createTblStatement = f'''IF OBJECT_ID(N'[dbo].[jt.{tableName}]', N'U') IS NULL 
    BEGIN
    CREATE TABLE [dbo].[jt.{tableName}]('''

    for i in range(len(columnDataType)):
        createTblStatement = createTblStatement + '\n' + f'[{columnName[i]}]' + ' ' + columnDataType[i] + ','

    createTblStatement = createTblStatement[:-1] + ')\nEND;'

    # creating the table in the SQL server
    cursor.execute(createTblStatement)
    cnxn.commit()

    placeholderValue = ','.join(['?'] * len(columnName))  # placeholder ?s for insert statement

    cursor.execute(f'DELETE FROM [dbo].[jt.{tableName}]')  # delete previous contents of table if there are any
    cnxn.commit()

    # replacing all NaN (missing values) with None, this appears to be neccessary as inserting into SQL seems to treat dataframe NaNs
    # as "nan" in text instead of SQL null values, which caused errors as it didn't recognize it as a float value when inserting
    df2 = df2.replace(np.nan, None)

    df2.to_sql(f"jt.{tableName}", engine, schema="dbo", method="multi", if_exists='replace', chunksize=150, index=False)


cursor.close()
