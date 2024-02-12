from simple_salesforce import Salesforce

#connect to Salesforce using User's security token, password, username, and org ID
def ConnectToSalesforce():
    
    # Old login for Josh's Account into main sandbox
    # sf = Salesforce(security_token='SGVAasqjCZWu6l0nuk2ykytDk', password='', username='josh.anderson@solutionsplusconsulting.com.golivedev', organizationId='00D3K0000008r2c', domain='test')
    
    #New login into partial backup, which is a second sandbox
    sf = Salesforce(security_token='AJo3pNmliGfhXraQPMqdohQAq', password='', username='salesforce@solutionsplusconsulting.com.partialsb', organizationId='00DE1000002GsWM', domain='test')
    


    #return the salesforce object
    #this will be passed around as needed
    return sf