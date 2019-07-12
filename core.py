'''
Authors: Chris Weakley | Adam Simon
Created: 7/11/19
Updated:
Description:
Core functionality of prepost compare module
'''
import compare

class PrePostComp(object):
    # contructor acceptis prechange, postchange, csr ids; these are required
    def __init__(self,prechange_id,postchange_id,csr_id,**kwargs):
        self.prechange_id = prechange_id
        self.postchange_id = postchange_id
        self.csr_id = csr_id
        # spreadsheet url is optional param, if passed grab if out of the url
        # if not passed set as empty strings, request at later date
        # TODO: do we want a default spreadsheet id, one we keep adding tabs to?
        if 'ss_url' in kwargs:
            self.spreadsheet_url = kwargs['ss_url']
            self.spreadsheetId = self.spreadsheet_url.split('/')[-2]
        else:
            self.spreadsheet_url = ''
            self.spreadsheetId = ''

        # as a part of constructer, connect to databases
        self.mysql_client = compare.InitSQLClient()
        self.fsidocprops = compare.InitMongoClient()
        # self.sql_serve_conn = compare.InitSqlServerClient()

    # string representation of class, useful when debugging
    def __repr__(self):
        return '<PrePost Object using pre as %s and post as %s for csr id %s>'  \
        '' % (self.prechange_id, self.postchange_id, self.csr_id)

    # TODO:
    # add GoogleAPIAuthorization function to class, or should this be on __init__
    # add GetDocProps
