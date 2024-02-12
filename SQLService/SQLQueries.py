import pyodbc
import SQLService.SQLConnect as SQL
import pandas as pd




#This function is used when it will return a single row back (typically a count)
def ExecuteOneRowSQL(conn, sql):

    cursor = conn.cursor()
    
    cursor.execute(sql)
    
    results = cursor.fetchone()[0]
    
    cursor.close()
    
    return results


#This function is used to return everything in the query 
#returns in a dataframe to be easier to work with
def ExecuteSQLReturnAll(conn, sql):
    
    cursor = conn.cursor()
    
    cursor.execute(sql)
    
    result = cursor.fetchall()
    
    cursor.close()
    
    result_df = pd.DataFrame(result)
    
    return result_df

#This function is used when executing multiple SQL Statements with no results needed
#Typically Updates and something similar
def ExecuteManySql(conn, sql, df):
    
    cursor = conn.cursor()

    cursor.fast_executemany = True

    cursor.executemany(sql, df)
    
    cursor.commit()
    
    cursor.close()

    return "Complete"


def ExecuteSQLNoReturn(conn, sql):
    
    cursor = conn.cursor()
    
    cursor.execute(sql)
    
    cursor.commit()
    
    cursor.close()
    
    return "Complete"