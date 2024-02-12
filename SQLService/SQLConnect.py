import pyodbc


def ConnectToSQL():
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};Database=SAGE;UID=sa;PWD=sa;SERVER=localhost')
    
    return conn