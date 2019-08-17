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
    def __init__(self,prechangeId,postchangeId,csrId,**kwargs):
        self.prechangeId = prechangeId
        self.postchangeId = postchangeId
        self.csrId = csrId

        # spreadsheet url is optional param, if passed grab if out of the url
        # if not passed set as empty strings, request at later date
        # TODO: do we want a default spreadsheet id, one we keep adding tabs to?
        if 'ssUrl' in kwargs:
            self.spreadsheetUrl = kwargs['ssUrl']
            self.spreadsheetId = self.spreadsheetUrl.split('/')[-2]
        else:
            self.spreadsheetUrl = ''
            self.spreadsheetId = ''

        # Optional Parameters and Default Values:
        # -preEnv and postEnv
        #   desc: environment batch was ran in
        #   values: imdb, reportdb
        # -compareLogic
        #   desc: logic used to match docs across batches
        #   values: docId, masterKey     
        # -masterKeyProps
        #   desc: doc props that will be used to match docs across batches if compareLogic = docId
        #   values: props listed and any user defined properties from free-form text box   
        optionalParams = {  'preEnv'        : 'imdb',   # or 'reportdb'
                            'postEnv'       : 'imdb',   # or 'reportdb'
                            'compareLogic'  : 'docId',  # or 'masterKey'
                            'masterKeyProps': ['ACCOUNT_NUMBER', 'INVOICE_NUMBER', 'TOTAL_DUE', 'BT_ROUTE', 'FFDID']} 

        for param in optionalParams.keys():
            if param in kwargs:
                setattr(self, param, kwargs[param])
            else:
                setattr(self, parm, optionalParams[param])                              

        # create arguments dict to pass to funcs
        self.arguments = {
            'custId'        : self.csrId,
            'preId'         : self.prechangeId,
            'preEnv'        : self.preEnv,
            'postId'        : self.postchangeId,
            'postEnv'       : self.postEnv,
            'spreadsheetURL': self.spreadsheetUrl,
            'spreadSheetId' : self.spreadsheetId,
            'compareLogic'  : self.compareLogic,
            'masterKeyProps': self.masterKeyProps
        }
        # as a part of constructer, connect to databases
        self.mysqlClient = compare.InitSQLClient()
        self.fsidocprops = compare.InitMongoClient()
        # self.sql_serve_conn = compare.InitSqlServerClient()

        # add google api build obj as class attr
        self.service = compare.GoogleAPIAuthorization()

        # grab coversheet ids
        self.coversheetDocIds = compare.GetCoversheetDocIds(self.mysqlClient,
                                                            self.arguments)
        # grab fsi doc info
        self.fsiDocumentInfo = compare.GetFSIDocumnetInfo(self.mysqlClient,
                                                          self.arguments)

    # string representation of class, useful when debugging
    def __repr__(self):
        return '<PrePost Object using pre as %s and post as %s for csr id %s>'  \
        '' % (self.prechangeId, self.postchangeId, self.csrId)
