import SalesforceFiles.SalesforceConnection as SFConnect
import SalesforceFiles.SalesforceQuery as SFQuery
import SQLService.SQLQueries as SQL
import SQLService.SQLConnect as SQLConnect

import pandas as pd
import numpy as np

conn = SQLConnect.ConnectToSQL()

####################################
### TESTING GETTING LIST OF FIELDS

# sf = SFConnect.ConnectToSalesforce()

# sql = "select Salesforce_Object, DB_Object from Salesforce_Object_Pairs"

# #create table statement to hold the salesforce metadata
# createStatement = """drop table if exists Salesforce_Metadata
    
#                         create table Salesforce_Metadata (
# 	                    ID int Identity(1,1) Primary Key
# 	                    ,Salesforce_Object varchar(200)
#                         ,Salesforce_Column_Name varchar(200)
#                         ,Salesforce_Column_Label varchar(200)
#                         ,Salesforce_Column_Type varchar(200)
#                         ,Salesforce_Column_Ref varchar(200)
#                         ,Salesforce_Column_External varchar(20)
#                         ,DB_Table varchar(200)
#                         ,DB_Column_Name varchar(200)
#                         ,DB_Column_Type varchar(200)
#                         ,Execution_Order int
#                     )"""

# result = SQL.ExecuteSQLReturnAll(conn, sql)

# SQL.ExecuteSQLNoReturn(conn, createStatement)
  
# #for each object in the list above, loop through one object at a time
# for row in result.iterrows():
#     sfObject = row[1].values[0][0]
#     dbObject = row[1].values[0][1]
    
#     print(sfObject, dbObject)
#     sfObjectOriginal = sfObject

#     if(sfObject in ["PricebookEntryA", "PricebookEntryB", "PricebookEntryC", "PricebookEntryCem", "PricebookEntryPick"]):
#         sfObjectOriginal = sfObject
#         sfObject = "PricebookEntry"
#     elif(sfObject == "Vendor"):
#         sfObjectOriginal = sfObject
#         sfObject = "Account"
        
    
    
#     # # allows to use the two variables in a parent.child relationship.  
#     # # It works great in this dynamic situation. 
#     # # describe then gets all the fields and info on each 
#     desc = getattr(sf, sfObject).describe()
    
    
    
#     fields = []
    
#     #Break up different sections into separate arrays
#     field_labels = [field['label'] for field in desc['fields']]
#     field_names = [field['name'] for field in desc['fields']]
#     field_types = [field['type'] for field in desc['fields']]    
#     field_ref = [field['referenceTo'] for field in desc['fields']]
#     field_ext = [field['externalId'] for field in desc['fields']]
                  
    
#     insertStatement = ""

#     #loop through all sets 
#     for label,name,dtype,ref,ext in zip(field_labels, field_names, field_types, field_ref, field_ext):
#         #add to the initally empty array fields
#         fields.append((label, name, dtype, ref, ext))
        
#         #update the insert statement
#         insertStatement = "insert into Salesforce_Metadata (Salesforce_Object, Salesforce_Column_Name, Salesforce_Column_Label, Salesforce_Column_Type, Salesforce_Column_Ref, Salesforce_Column_External, DB_Table) values('" + str(sfObjectOriginal) + "', '" + name + "','" + label + "', '" + dtype + "', '" + ''.join(ref) + "', '" + str(ext) + "', '" + dbObject + "')"
#         #execute the insert statement back into SQL
#         result = SQL.ExecuteSQLNoReturn(conn, insertStatement)
        
# ######################################################

# sql = "select distinct Salesforce_Object, DB_Table from Salesforce_Metadata where DB_Table is not null"   
    
# result = SQL.ExecuteSQLReturnAll(conn, sql)    

# for row in result.iterrows():
#     sfObject = row[1].values[0][0]
#     dbObject = row[1].values[0][1]
    
#     sfSQL = "select Salesforce_Object, Salesforce_Column_Label, Salesforce_Column_Name, Salesforce_Column_Type, Salesforce_Column_Ref, Salesforce_Column_External, DB_Table, DB_Column_Name, DB_Column_Type from Salesforce_Metadata where Salesforce_Object = '" + sfObject + "'"

#     columnSQL = "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where table_name = '" + dbObject + "' order by ORDINAL_POSITION"

#     df = pd.read_sql(columnSQL, conn)

#     sql = "select "

#     for row in df.iterrows():
#         name = row[1].values[0]
#         sql += name + ","
        
#     sql = sql[0:len(sql)-1]

#     sql += " from " + dbObject

#     # print(sql)

#     df = pd.read_sql(sql, conn)

#     # for col in df.columns:
#     #     print(col)


#     sfFields = pd.read_sql(sfSQL, conn)


#     for col, dtype in zip(df.columns, df.dtypes):
#         # print(col, dtype)
#         for index, row in sfFields.iterrows():
#             if row['Salesforce_Column_Name'] == col:
#                 # print("MATCH", col, dtype)
#                 updateString = "update Salesforce_Metadata "
#                 updateString += "set DB_Table = '"+ dbObject + "' "
#                 updateString += ",DB_Column_Name = '" + col + "' "
#                 updateString += ",DB_Column_Type = '" + str(dtype) + "' "
#                 updateString += "where Salesforce_Object = '" + sfObject + "' "
#                 updateString += "and Salesforce_Column_Name = '" + row['Salesforce_Column_Name'] + "'"
#                 # print(updateString)
#                 SQL.ExecuteSQLNoReturn(conn, updateString)
                        
            
            
            
####################################################
# Finish the whole process

####################################
### Need to build a table and then build array from it
###################################

# sql = "select distinct Salesforce_Object, DB_Table from Salesforce_Metadata where DB_Table is not null and DB_Table in ('State_Tax', 'Terms', 'Zones', 'Colors')"   
sql = "select distinct Salesforce_Object, DB_Table, Execution_Order from Salesforce_Metadata where DB_Table is not null and DB_Table in ('Invoices') order by Execution_Order "   

result = SQL.ExecuteSQLReturnAll(conn, sql)    

for row in result.iterrows():
    sfObject = row[1].values[0][0]
    dbObject = row[1].values[0][1]

    sf = SFConnect.ConnectToSalesforce()
    # # Had to do this since we have different pricebooks
    ### NEED TO ADD MORE LOGIC TO MAKE SURE IT DELETES ENTRIES FROM CORRECT PRICE BOOK ####
    if(sfObject in ['PricebookEntryPick', 'PricebookEntryCem', 'PricebookEntryC', 'PricebookEntryB', 'PricebookEntryA']):
        sqol = "select id from PricebookEntry"
    else:
        sqol = "select id from " + sfObject

    df = SFQuery.QuerySalesforce(sf, sqol)

    sfObjectOriginal = sfObject

    for row in df.iterrows():
        sfID = row[1].values[1]
        if(sfObject in ['PricebookEntryPick', 'PricebookEntryCem', 'PricebookEntryC', 'PricebookEntryB', 'PricebookEntryA']):
            sfObjectOriginal = sfObject
            sfObject = "PricebookEntry"
            getattr(sf, sfObject).delete(sfID)

        else:
            getattr(sf, sfObject).delete(sfID)
    
        
    
    if(sfObject in ['PricebookEntryPick', 'PricebookEntryCem', 'PricebookEntryC', 'PricebookEntryB', 'PricebookEntryA']):
        columnSQL = "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where table_name = 'PricebookEntry' order by ORDINAL_POSITION"
    else:
        columnSQL = "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where table_name = '" + dbObject + "' order by ORDINAL_POSITION"

    dbColumns = pd.read_sql(columnSQL, conn)

    sql = "select top 10 "

    for row in dbColumns.iterrows():
        name = row[1].values[0]
        sql += name + ","
        
    sql = sql[0:len(sql)-1]

    sql += " from " + dbObject

    dbRows = pd.read_sql(sql, conn)

    if(sfObject in ['PricebookEntryPick', 'PricebookEntryCem', 'PricebookEntryC', 'PricebookEntryB', 'PricebookEntryA']):
        sfSQL = "select Salesforce_Object, Salesforce_Column_Label, Salesforce_Column_Name, Salesforce_Column_Type, Salesforce_Column_Ref, Salesforce_Column_External, DB_Table, DB_Column_Name, DB_Column_Type from Salesforce_Metadata where Salesforce_Object = 'PricebookEntry' and DB_Column_Name is not null and Salesforce_Column_Ref <> '' and Salesforce_Column_Name <> 'RecordTypeId'"
    else:
        sfSQL = "select Salesforce_Object, Salesforce_Column_Label, Salesforce_Column_Name, Salesforce_Column_Type, Salesforce_Column_Ref, Salesforce_Column_External, DB_Table, DB_Column_Name, DB_Column_Type from Salesforce_Metadata where Salesforce_Object = '" + sfObject + "' and DB_Column_Name is not null and Salesforce_Column_Ref <> '' and Salesforce_Column_Name <> 'RecordTypeId'"

    sfFields = pd.read_sql(sfSQL, conn)

    createString = []

    my_dict = {}
    
                
    # # Loop through Rows and Columns
    # # create an empty dictionary for each row
    # # The Column is the Key and then the value is pulled from the row
    # # The key is used to add the value to the dictionary
    # # Added to an array, which makes it like a JSON

    

    for i in range(len(dbRows)):
        my_dict = {}
        for j in range(len(dbRows.columns)):
            key = dbRows.columns[j]
            value = dbRows.iloc[i, j]
            if(key in sfFields['DB_Column_Name'].values.tolist()):
                # print(key, "Lookup Object ", ''.join(sfFields[sfFields['DB_Column_Name']==key]['Salesforce_Column_Ref'].values.tolist()), value)
                lookup = ''.join(sfFields[sfFields['DB_Column_Name']==key]['Salesforce_Column_Ref'].values.tolist())
                column = ''.join(sfFields[sfFields['DB_Column_Name']==key]['Salesforce_Column_Name'].values.tolist())
                
                sql = "select top 1 Salesforce_Column_Name from Salesforce_Metadata where Salesforce_Object = '" + lookup + "' and Salesforce_Column_External = 'True'"
                # print("MY SQL -- ", sql)
                
                               
                results = SQL.ExecuteOneRowSQL(conn, sql)
                
                Id = getattr(sf, lookup).get_by_custom_id(results, value)
                # print(Id['Id'])
                value = Id['Id']
                
            my_dict[key] = value
        
        
        # my_dict['Customer__c'] = '0013K00000yLkWHQA0'
        
        if(sfObject == 'Invoice__c'):
            my_dict['Master_Account__c'] = '0013K00000y4z5PQAQ'        
            
        
        if(sfObject == 'Account'):
            my_dict['Master_Customer__c'] = '0013K00000y4z5PQAQ'        
        
        createString.append(my_dict)
    
    
        
            
    # print(createString)

    sf.bulk.__getattr__(sfObject).insert(createString, batch_size=100000, use_serial=True)





            