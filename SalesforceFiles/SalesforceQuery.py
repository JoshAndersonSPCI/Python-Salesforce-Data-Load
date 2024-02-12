from simple_salesforce import Salesforce
import pandas as pd

def QuerySalesforce(sf, query):
    
    #query Salesforce with provided query
    data = sf.query_all(query)
    
    #convert to Pandas DataFrame
    data_df = pd.DataFrame(data['records'])
    
    #return the dataframe
    return data_df