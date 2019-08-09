from __future__ import print_function,unicode_literals
from pprint import pprint
import pickle
import pandas as pd
import os.path
import time
import sys
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient import discovery
from appJar import gui
import pymongo
from pymongo import MongoClient
from pymongo import MongoReplicaSetClient
from pprint import pprint
import gspread_dataframe as gd
# User Info
import os
import json
from itertools import tee
from collections import OrderedDict
import mysql.connector
from base64 import b64decode
import pyodbc

def InitSQLClient():
    userName = os.getenv('username')
    sqlConnFile = r"C:\\Users\\%s\\AppData\\Roaming\\SQLyog\\sqlyog.ini" % userName
    inFile = open(sqlConnFile, 'rt')
    foundConnection = False
    connections = {}
    for line in inFile.readlines():
        if "[Connection" in line:
            foundConnection = True
            currentConnection = line.strip()
            connections[currentConnection] = {}
        if foundConnection:
            if line.startswith("Host="):
                connections[currentConnection]["Host"] = line.split("=")[1].strip()
            elif line.startswith("User="):
                connections[currentConnection]["User"] = line.split("=")[1].strip()
            elif line.startswith("Password="):
                connections[currentConnection]["Password"] = line.split("=")[1].strip()
    inFile.close()
    userName = ""
    password = ""
    for connection in connections.values():
        if "Host" in connection:
            if connection["Host"].upper() == "IMDB":
                if "User" in connection:
                    userName = connection["User"]
                if "Password" in connection:
                    password = connection["Password"]
    #print(userName, password)

    # SQLyog stores passwords with base 64 encoding so we must decode it
    decodedPassword = decode_password(password)

    im_sqlClient = mysql.connector.connect(
        host="imdb",
        user=userName,
        passwd=decodedPassword,
        database="imstage01"
    )
    prod_sqlClient = mysql.connector.connect(
        host="reportdb",
        user=userName,
        passwd='nF3vGBQkbtVYuRg',
        database="prod"
    )        
    return im_sqlClient, prod_sqlClient

def decode_password(encoded):
    # print('encoded password is %s' % encoded)
    # see https://gist.github.com/perrygeo/ee7c65bb1541ff6ac770
    if len(encoded) % 4 != 0:
        if len(encoded) % 4 == 2:
            encoded += '==='
        elif len(encoded) % 4 == 1:
            encoded += '=='
        elif len(encoded) % 4 == 3:
            encoded += '='
    tmp = bytearray(b64decode(encoded))
    for i in range(len(tmp)):
        tmp[i] = rotate_left(tmp[i], 8)
    return tmp.decode('utf-8')

def rotate_left(num, bits):
    bit = num & (1 << (bits-1))
    num <<= 1
    if(bit):
        num |= 1
    num &= (2**bits-1)
    return num

def GetCoversheetDocIds(im_sqlClient, prod_sqlClient, arguments):
    # We want to ignore any document that was created as a coversheet
    #if prod_sqlClient != '':
    #    sqlCursor = prod_sqlClient.cursor()
    #else:     
    sqlCursor = im_sqlClient.cursor()
    # Prechange
    sqlCursor.execute("SELECT documentId FROM fsidocument WHERE customerid = %s AND batchid = %s \
                      AND (FFDId IN (SELECT FFDId FROM fsiFFD WHERE customerId = %s AND itemType = 'O') \
                      OR FFDId = 88908)" % (arguments['custId'], arguments['preId'], arguments['custId']))
    preCoversheetDocIds = list(int(i[0]) for i in sqlCursor.fetchall()) # convert from tuple generator of Longs to Int list
    
    # Postchange
    sqlCursor = im_sqlClient.cursor()
    sqlCursor.execute("SELECT documentId FROM fsidocument WHERE customerid = %s AND batchid = %s \
                      AND (FFDId IN (SELECT FFDId FROM fsiFFD WHERE customerId = %s AND itemType = 'O') \
                      OR FFDId = 88908)" % (arguments['custId'], arguments['postId'], arguments['custId']))
    postCoversheetDocIds = list(int(i[0]) for i in sqlCursor.fetchall()) # convert from tuple generator of Longs to Int list
    #print(len(preCoversheetDocIds))
    coversheetDocIds = (preCoversheetDocIds, postCoversheetDocIds)
    return (coversheetDocIds)

def GetFSIDocumnetInfo(im_sqlClient, arguments, prod_sqlClient=''):

    if prod_sqlClient != '':
        sqlCursor = prod_sqlClient.cursor()
    else:     
        sqlCursor = im_sqlClient.cursor()

    destTypes = {"C" : "Coversheet",
                 "D" : "Print and Ebill",
                 "E" : "Ebill",
                 "I" : "Invoice Central",
                 "G" : "OB 10",
                 "P" : "Pull - P",
                 "Q" : "Pull - Q",
                 "R" : "Pull - R",
                 "S" : "Print",
                 "T" : "Pull - T",
                 "V" : "Email",
                 "X" : "Fax"}
    # Prechange
    sqlCursor.execute("SELECT documentId, FFDId, DestType, PageCount FROM fsidocument WHERE customerid = %s AND batchid = %s \
                      AND (FFDId NOT IN (SELECT FFDId FROM fsiFFD WHERE customerId = %s AND itemType = 'O') \
                      AND FFDId != 88908)" % (arguments['custId'], arguments['preId'], arguments['custId']))
    preBatchInfo = {}
    for document in sqlCursor.fetchall():
        preBatchInfo[str(document[0])] = {"FFDID"    : str(document[1]),
                                          "BT_ROUTE" : destTypes[str(document[2])],
                                          "PAGECOUNT": str(document[3])}
    # Postchange
    sqlCursor = im_sqlClient.cursor()    
    sqlCursor.execute("SELECT documentId, FFDId, DestType, PageCount FROM fsidocument WHERE customerid = %s AND batchid = %s \
                      AND (FFDId NOT IN (SELECT FFDId FROM fsiFFD WHERE customerId = %s AND itemType = 'O') \
                      AND FFDId != 88908)" % (arguments['custId'], arguments['postId'], arguments['custId']))
    postBatchInfo = {}
    for document in sqlCursor.fetchall():
        postBatchInfo[str(document[0])] = {"FFDID"    : str(document[1]),
                                           "BT_ROUTE" : destTypes[str(document[2])],
                                           "PAGECOUNT": str(document[3])}
    if preBatchInfo == {}:
        print("Did not find any record in fsidocument for prebatch customerId: %s, batchId: %s" % (arguments['custId'], arguments['preId']))
        sys.exit()
    elif postBatchInfo == {}:
        print("Did not find any record in fsidocument for postbatch customerId: %s, batchId: %s" % (arguments['custId'], arguments['postId']))
        sys.exit()
    else:
        return (preBatchInfo, postBatchInfo)

def GoogleAPIAuthorization():
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token, encoding='latin1')
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
        # create google api build obj to exeecute calls against
    service = discovery.build('sheets', 'v4', credentials=creds)
    return service

# Write a single range of values out
def UpdateSingleRange(values, startPos, sheetName, spreadsheetId, printData=False, value_input_option="RAW", insertDataOption="OVERWRITE"):
    #value_input_option = "RAW" #input raw string data, no formulas, dates, currency, ect.
    startPos = sheetName + '!' + startPos
    print("Updating spreadsheet id: %s" % spreadsheetId)
    print("Starting at cell: %s" % startPos)
    # input paramater 'values' holds a list of row data, lets update 1000 rows at a time (only use even numbers)
    rowCount = len(values)
    rowsPerUpdate = 1000
    for i in range(0, rowCount, rowsPerUpdate):
        if i + rowsPerUpdate > rowCount: # add remaining orphan updates
            print("Updating rows %s through %s" % (str(i), str(rowCount)))
            body = {'values': values[i:rowCount]}
        else: # add batch updates
            print("Updating rows %s through %s" % (str(i), str(i + rowsPerUpdate)))
            if printData:
                print(values)
            body = {'values': values[i:i + rowsPerUpdate]}
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheetId, range=startPos,
            valueInputOption=value_input_option, body=body).execute()
        print('{0} cells updated.'.format(result.get('updatedCells')))
        # update starting position
        startPos = startPos.split("!")[1]
        startCol = list(filter(str.isalpha, str(startPos)))[0]
        startRow = str(int(list(filter(str.isdigit, str(startPos)))[0]) + rowsPerUpdate)
        startPos = sheetName + '!' + startCol + startRow

<<<<<<< HEAD
#########################
def InitMongoClient(queryProd = False):
=======

def InitMongoClient():
>>>>>>> da03421a5423574cb3d732bf8c2094eff7920f91
    ###############################
    # START: GET USER CREDENTIALS #
    userName = os.getenv('username')
    userPassword = ''
    roboPath = "C:\\Users\\%s\\.3T\\robo-3t\\1.3.1\\robo3t.json" % userName

    print ("Validating user credentials...")
    try:
        with open(roboPath) as json_file:
            connectionData = json.load(json_file)
            for connection in connectionData['connections']:
                # AS_061719: update below line to check for different hosts
                if connection['serverHost'].split('.')[0] in ("ssnj-immongodb01","ssnj-immongodb02","ssnj-immongodb03"):
                    for cred in connection['credentials']:
                        userName = cred['userName']
                        userPassword = cred['userPassword']
    except:
        userName = "cweakley"
        userPassword = "vn6oCdWK"


    # END: GET USER CREDENTIALS #
    #############################

    ##################################
    # START: CONNECT TO MONGO CLIENT #
    print ("Connecting to mongo client...")
    imsMongoClient = MongoReplicaSetClient(["ssnj-immongodb01:10001", "ssnj-immongodb02:10001", "ssnj-immongodb03:10001"],
                                            userName = userName,
                                            password = userPassword,
                                            authSource = 'docpropsdb',
                                            authMechanism = 'SCRAM-SHA-1')
    im_fsidocprops = imsMongoClient.docpropsdb.fsidocprops

    prod_fsidocprops = None 
    if queryProd:
        prodMongoClient = MongoClient(["prmreportdb01:10001"],
                                                userName = userName,
                                                password = 'gEvSnvCy',
                                                authSource = 'docpropsdb',
                                                authMechanism = 'SCRAM-SHA-1')
        prod_fsidocprops = prodMongoClient.docpropsdb.fsidocprops
    return im_fsidocprops, prod_fsidocprops

    # END: CONNECT TO MONGO CLIENT #
    ################################

    ##################################
    #  START: CONNECT TO SQL sERVER  #

def InitSqlServerClient(server='dnco-stc2bsql.billtrust.local',database='carixDataProcessing',trusted_conn_bool='yes'):
    print('Connecting using windows auth...')
    conn = pyodbc.connect(driver='{SQL Server}',
                          server=server,
                          database=database)
    cursor = conn.cursor()
    return cursor
    print('Connected, cursor object returned...')
    #   END: CONNECT TO SQL SERVER   #
    ##################################

def GetDocProps(im_fsidocprops, coversheetDocIds, arguments, prod_fsidocprops=''):
    # START: QUERY FOR DP #
    #######################
    #preId = int(app.getEntry('ePrechangeId'))
    #postId = int(app.getEntry('ePostchangeId'))
    #custId = int(app.getEntry('eCustId'))
    print("Time Elapsed: %s" % (time.time() - startTime))
    print("Querying for prechange and postchange doc props...")

    #if prod_fsidocprops != '' or prod_fsidocprops is not None:
    #    fsidocprops = prod_fsidocprops
    #else:
    fsidocprops = im_fsidocprops    
    prechangeProps = fsidocprops.find({'batchId': arguments['preId'], 'customerId': arguments['custId'], 'documentId': {'$nin': coversheetDocIds[0]}})
    #fsidocprops = im_fsidocprops
    postchangeProps = fsidocprops.find({'batchId': arguments['postId'], 'customerId': arguments['custId'], 'documentId': {'$nin': coversheetDocIds[1]}})
    print("Query successful...")
    prechangeProps = list(prechangeProps)
    postchangeProps = list(postchangeProps)
    #sys.exit()
    #print(prechangeProps)
    # END: QUERY FOR DP #
    #####################
    print("Query finished... CustomerId: %s  Prechange: %s  Postchange: %s" % (str(arguments['custId']), str(arguments['preId']), str(arguments['postId'])))
    #print("Number of Prechange documents: %s" % len(prechangeProps))
    #print("Number of Postchange documents: %s" % len(postchangeProps))
    print("Time Elapsed: %s" % (time.time() - startTime))

    return(prechangeProps, postchangeProps)

# Was going to try out Pandas with this function but then I got lazy
def QueryMongo(coversheetDocIds, fsidocprops, arguments):
    #fsidocprops = InitMongoClient()
    # START: QUERY FOR DP #
    #######################
    #preId = int(app.getEntry('ePrechangeId'))
    #postId = int(app.getEntry('ePostchangeId'))
    #custId = int(app.getEntry('eCustId'))
    print("Time Elapsed: %s" % (time.time() - startTime))
    print("Querying for prechange and postchange doc props...")

    #pd.set_option('display.max_columns', 500)

<<<<<<< HEAD
    prechangePropsGen = fsidocprops.find({'batchId': arguments['preId'], 'customerId': arguments['custId'], 'documentId': {'$nin': coversheetDocIds[0]}},
        {'_id':0, 'batchId':0, 'customerId':0, 'size':0, 'seq':0, 'lockId':0})
    postchangePropsGen = fsidocprops.find({'batchId': arguments['postId'], 'customerId': arguments['custId'], 'documentId': {'$nin': coversheetDocIds[1]}},
        {'_id':0, 'batchId':0, 'customerId':0, 'size':0, 'seq':0, 'lockId':0})
=======
    prechangePropsGen = fsidocprops.find({'batchId': preId,
                                          'customerId': custId,
                                          'documentId': {'$nin': coversheetDocIds[0]}},
                                         {'_id':0,
                                          'batchId':0,
                                          'customerId':0,
                                          'size':0,
                                          'seq':0,
                                          'lockId':0})
    postchangePropsGen = fsidocprops.find({'batchId': postId,
                                           'customerId': custId,
                                           'documentId': {'$nin': coversheetDocIds[1]}},
                                          {'_id':0,
                                           'batchId':0,
                                           'customerId':0,
                                           'size':0,
                                           'seq':0,
                                           'lockId':0})
>>>>>>> da03421a5423574cb3d732bf8c2094eff7920f91

    '''
    test = pd.DataFrame()
    for prop in prechangeDf['properties']:
        test.
    print (test)
    sys.exit()

    print (prechangeDf)

    #prechangeDf.drop(ignoreColumns, axis=1, inplace=True)
    #for col in ignoreColumns: del prechangeDf[col]

    for doc in prechangeDf['properties']:
        count = 0
        for prop in doc:
            if "_COL" not in prop.get('k'):
                count += 1
        print(count)
    '''

    #print(prechangeProps.size)
    #print(prechangeProps.columns)
    #postchangeProps = list(fsidocprops.find({'batchId': postId, 'customerId': custId}))#,{'_id':0, 'properties':1}))
    #print(prechangeProps)
    # END: QUERY FOR DP #
    #####################
    print("Query finished... CustomerId: %s  Prechange: %s  Postchange: %s" % (arguments['custId'], arguments['preId'], arguments['postId']))
    print("Time Elapsed: %s" % (time.time() - startTime))

    return(prechangePropsGen, postchangePropsGen)
    #sys.exit()
    #################
    # TEST FUNCTION #
    #MergeToDataFrame(prechangePropsGen, postchangePropsGen)

def MergeBatchData(prechangeProps, postchangeProps, preBatchInfo, postBatchInfo):
    print("Starting MergerBatchData...")
    fsiDocumentProps = ["FFDID", "BT_ROUTE", "PAGECOUNT"]
    docPropLabels = ["FFDID", "BT_ROUTE", "PAGECOUNT"]
    # Add all doc props from our prechange and postchange batches to a list of doc prop names
    for batch in (prechangeProps, postchangeProps):
        for document in batch:
            for prop in document.get('properties'):
                docPropName = prop.get('k')
                if docPropName: # Do not add columnar properties or special biscuit generated properties.. XML_DATA was causing a failure
                    if not docPropName.endswith("_COL") and docPropName not in ignoreTheseDocProps:
                        if docPropName not in docPropLabels:
                            docPropLabels.append(str(docPropName))

    # Sort the labels and add DOCUMENTID, ACCOUNT_NUMBER and INVOICE_NUMBER to the front\
    docPropLabels.sort()
    props = ["BT_ROUTE", "INVOICE_NUMBER", "ACCOUNT_NUMBER"]
    #props = ["ACCOUNT_NUMBER", "TOTAL_DUE"]
    for prop in props:
        if prop in docPropLabels:
            docPropLabels.remove(prop)
            docPropLabels.insert(0, prop)
    docPropLabels.insert(0, "")
    docPropLabels.insert(0, "DOCUMENTID")

    # masterPropList will contain our final structure of pre and post doc props with a masterKey
    # masterKey is currently ''.join(ACCOUNT_NUMBER INVOICE_NUMBER) but should optionally be user defined
    # {ACCOUNT_NUMBERINVOICE_NUMBER: [prechangePropValue, postchangePropValue], [prechangePropValue, postchangePropValue], ...}
    masterPropList = OrderedDict()
    # START PRECHANGE PROPS
    # Doc props can be split across multiple mongo Objects, this means documentId cannot be used as a unique identifier
    # Here we remove and combine these split db objects and add them back into our original list
    # See Example: db.getCollection('fsidocprops').find({"customerId":2001, "batchId":13811669, "documentId":4315315279})
    for batch in (prechangeProps, postchangeProps):
        splitObjects = {}
        removeThese = []
        for i, document in enumerate(batch):
            if document.get('pages') > 1:
                docId = document.get('documentId')
                removeThese.append(i)
                if docId not in splitObjects:
                    splitObjects[docId] = document
                else:
                    splitObjects[docId].get('properties').extend(document.get('properties'))
        for index in sorted(removeThese, reverse = True):
            batch.pop(index)
        for docId in splitObjects.values():
            batch.extend([docId])

    count = 0
    # These are the doc props that will be used as a unique key to match up pre/post documents
    # In the future this should be defaulted to acc num and inv num with the option for user override
    #propKeys = ["ACCOUNT_NUMBER", "INVOICE_NUMBER"]  "PO_NUMBER", "BT_ROUTE", "TOTAL_DUE"
    propKeys = ["ACCOUNT_NUMBER", "INVOICE_NUMBER", "FFDID", "FILENAME", "TOTAL_DUE"]
    for document in prechangeProps:
        # Get master key before starting
        masterKey = []
        for prop in propKeys:
            if prop in fsiDocumentProps:
                masterKey.append(preBatchInfo[str(document.get('documentId'))].get(prop))
            else:
                for docProp in document.get('properties'):
                    if prop == docProp.get('k'):
                        if prop == "FILENAME":
                            masterKey.append(docProp.get('v').split("\\")[-1])
                        else:        
                            masterKey.append(docProp.get('v'))
        #print(masterKey)
        masterKey = '~'.join(masterKey)
        count += 1

        #print(masterKey)
        #sys.exit()


        # Exit if we were not able to find either account number or invoice number
        if masterKey == '':
            print("Not able to find any doc prop keys in the prechange batch")
            print(str(count))
            sys.exit()
        elif masterKey in masterPropList:
            print("Found a duplicate masterkey within the prechange batch: ", masterKey)
            print("Master key components:", '~'.join(propKeys))
            sys.exit()

        #print (docProps)
        for docPropLabel in docPropLabels:
            if docPropLabel == "DOCUMENTID":
                masterPropList[masterKey] = [[str(document.get('documentId')), '']]
            elif docPropLabel in fsiDocumentProps:
                #print(preBatchInfo[str(document.get('documentId'))][docPropLabel])
                masterPropList[masterKey].append([preBatchInfo[str(document.get('documentId'))][docPropLabel], ''])
            else:
                tempPropValues = ['', '']
                if docPropLabel != "":
                    for prop in document.get('properties'):
                        propName = prop.get('k')
                        if propName == docPropLabel:
                            if propName == "FILENAME":
                                tempPropValues[0] = prop.get('v').replace('<BR>', '\n')[:5000].split("\\")[-1]
                            else:   
                                tempPropValues[0] = prop.get('v').replace('<BR>', '\n')[:5000] #google sheets limits cell data to 5000 chars
                            break
                masterPropList[masterKey].append(tempPropValues)
        # END PRECHANGE PROPS

    # START POSTCHANGE PROPS
    misMatchCount = 0
    for document in postchangeProps:
        misMatch = False
        # Get master key before starting
        masterKey = []
        for prop in propKeys:
            if prop in fsiDocumentProps:
                masterKey.append(postBatchInfo[str(document.get('documentId'))].get(prop))
            else:
                for docProp in document.get('properties'):
                    if prop == docProp.get('k'):
                        if prop == "FILENAME":
                            masterKey.append(docProp.get('v').split("\\")[-1])
                        else:        
                            masterKey.append(docProp.get('v'))                        
        masterKey = '~'.join(masterKey)

        # Exit if we were not able to find either account number or invoice number
        if masterKey == '':
            print("Not able to find either account number or invoice number in the postchange batch")
            sys.exit()
        elif not masterKey in masterPropList:
            print("Postchange masterkey not found in prechange masterkeylist, adding mismatched key.")
            print(masterKey)
            misMatchCount += 1
            masterPropList[masterKey] = []
            for label in docPropLabels:
                masterPropList[masterKey].append(['',''])
            misMatch = True

        for i, docPropLabel in enumerate(docPropLabels):
            if docPropLabel == "DOCUMENTID":
                masterPropList[masterKey][i][1] = str(document.get('documentId'))
            elif docPropLabel in fsiDocumentProps:
                masterPropList[masterKey][i][1] = postBatchInfo[str(document.get('documentId'))][docPropLabel]
            elif docPropLabel != "":
                for prop in document.get('properties'):
                    propName = prop.get('k')
                    if propName == docPropLabel:
                        if propName == "FILENAME":
                            masterPropList[masterKey][i][1] = prop.get('v').replace('<BR>', '\n')[:5000].split("\\")[-1] 
                        else:    
                            masterPropList[masterKey][i][1] = prop.get('v').replace('<BR>', '\n')[:5000] #google sheets limits cell data to 5000 chars
                        break

        if misMatchCount > ((len(prechangeProps) + len(postchangeProps)) / 4) \
          or misMatchCount > len(prechangeProps) * .75 \
          or misMatchCount > len(postchangeProps) * .75:
            print("ERROR: More than half of the total document count are mismatched, or more than 75% of either the pre or post change documents " \
                  "are mismatched, check prechange and postchange batch ids.")
            sys.exit()
    print(misMatchCount, len(prechangeProps), len(postchangeProps))
    print("Time Elapsed: %s" % (time.time() - startTime))

    return(docPropLabels, masterPropList, misMatchCount, len(prechangeProps), len(postchangeProps))

def MergeToDataFrame(prechangePropsGen, postchangePropsGen, preBatchInfo, postBatchInfo, arguments):
    print("Starting MergeToDataFrame...")
    fsiDocumentProps = ["FFDID", "BT_ROUTE", "PAGECOUNT"]
    docPropLabels = ["FFDID", "BT_ROUTE", "PAGECOUNT"]

    prechangeProps = {}
    postchangeProps = {}
    batchData = {"preBatchCount": 0,
                 "postBatchCount": 0,
                 "fileList": set(),}
    fileList = {}             
    # Add all doc props from our prechange and postchange batches to a list of doc prop names
    isPrechangeLoop = True
    for batch in (prechangePropsGen, postchangePropsGen):
        # Doc props can be split across multiple mongo Objects, this means documentId cannot be used as a unique identifier
        # Here we remove and combine these split db objects and add them back into our original list
        # See Example: db.getCollection('fsidocprops').find({"customerId":2001, "batchId":13811669, "documentId":4315315279})
        splitObjects = {} # keep track of documents that are split across multiplie mongo objects
        for document in batch:
            isSplitObject = False
            docProps = {} # used to temp store our docProp label and values
            docId = str(document.get('documentId'))
            docProps['DOCUMENTID'] = docId

            # Keep track of how many documents are in each batch
            if isPrechangeLoop:
                batchData["preBatchCount"] += 1
            else:
                batchData["postBatchCount"] += 1    

            for prop in document.get('properties'):
                docPropName = prop.get('k').strip()
                if docPropName: # Do not add columnar properties or special biscuit generated properties.. XML_DATA was causing a failure
                    if "s" in prop: # Once we find the first columnar property we can break 
                        #print("Found first col prop, breaking properties loop at: %s, Line: %s" % (docPropName, prop.get('s')))
                        break
                    elif docPropName not in ignoreTheseDocProps:
                        if docPropName == "FILENAME":
                            docProps[docPropName] = prop.get('v').split("\\")[-1]  # only grab the file name and not the full path
                            batchData["fileList"].add(docProps[docPropName])
                        else:   
                            docProps[docPropName] = prop.get('v').replace('<BR>', '\n')[:5000]
                        # Create a list of all unique doc prop names across both pre and post
                        if docPropName not in docPropLabels:
                            docPropLabels.append(str(docPropName))
            # Add our fsiDocument values
            for docPropName in fsiDocumentProps:
                if isPrechangeLoop:
                    docProps[docPropName] = preBatchInfo[docId][docPropName]
                else:
                    docProps[docPropName] = postBatchInfo[docId][docPropName]

            # if pages > 1, document is split across multiple mongo objects and should be combined
            if document.get('pages') > 1:
                if docId not in splitObjects:
                    splitObjects[docId] = docProps
                else: 
                    splitObjects[docId].update(docProps)
                    # Each time we find a split doc we need to subtract one from our total doc count  
                if isPrechangeLoop:
                    batchData["preBatchCount"] -= 1
                else:
                    batchData["postBatchCount"] -= 1  
            else:
                # Add to our OrderedDict with filename and docid as the key which can be used to properly sort objects
                if isPrechangeLoop:
                    if docProps['FILENAME'] in prechangeProps:
                        prechangeProps[docProps['FILENAME']].update({docProps['DOCUMENTID']:docProps})
                    else:
                        prechangeProps[docProps['FILENAME']] = {docProps['DOCUMENTID']:docProps}        
                else:
                    if docProps['FILENAME'] in postchangeProps:
                        postchangeProps[docProps['FILENAME']].update({docProps['DOCUMENTID']:docProps})
                    else:    
                        postchangeProps[docProps['FILENAME']] = {docProps['DOCUMENTID']:docProps}

        # clean up our splitObjects
        for documentId in splitObjects:
            if isPrechangeLoop:
                if splitObjects[documentId]['FILENAME'] in prechangeProps:
                    prechangeProps[splitObjects[documentId]['FILENAME']].update({documentId:splitObjects[documentId]})
                else:
                    prechangeProps[splitObjects[documentId]['FILENAME']] = {documentId:splitObjects[documentId]}
            else:
                if splitObjects[documentId]['FILENAME'] in postchangeProps:
                    postchangeProps[splitObjects[documentId]['FILENAME']].update({documentId:splitObjects[documentId]})
                else:     
                    postchangeProps[splitObjects[documentId]['FILENAME']] = {documentId:splitObjects[documentId]}

        isPrechangeLoop = False


    # Sort the labels and add DOCUMENTID, ACCOUNT_NUMBER and INVOICE_NUMBER to the front
    docPropLabels.sort()
    props = ["BT_ROUTE", "INVOICE_NUMBER", "ACCOUNT_NUMBER"]
    for prop in props:
        if prop in docPropLabels:
            docPropLabels.remove(prop)
            docPropLabels.insert(0, prop)
    docPropLabels.insert(0, "")
    docPropLabels.insert(0, "DOCUMENTID")


    # Time to create our masterPropList which should be a 2d array structured exactly how our data will appear as a table in gsheets
    masterPropList = []
    i = 1
    for fileName in prechangeProps:
        #print("length of prechange docs: " + str(len(prechangeProps[fileName])))
        for docId in sorted(prechangeProps[fileName].keys()):
            propList = []
            for docPropLabel in docPropLabels:
                if docPropLabel in prechangeProps[fileName][docId]:
                    propList.append(prechangeProps[fileName][docId][docPropLabel])
                else:
                    propList.append("")
            masterPropList.extend([propList, []])
        if fileName in postchangeProps:
            #print("length of postchange docs: " + str(len(postchangeProps[fileName])))
            for docId in sorted(postchangeProps[fileName].keys()):
                propList = []
                for docPropLabel in docPropLabels:
                    if docPropLabel in postchangeProps[fileName][docId]:
                        propList.append(postchangeProps[fileName][docId][docPropLabel])
                    else:
                        propList.append("")       
                masterPropList[i] = propList
                i += 2

    for fileName in postchangeProps:
        if fileName not in prechangeProps:
            #masterPropList.extend([[],[]])
            print(len(masterPropList))
            for docId in sorted(postchangeProps[fileName].keys()):
                propList = []
                for docPropLabel in docPropLabels:
                    if docPropLabel in postchangeProps[fileName][docId]:
                        propList.append(postchangeProps[fileName][docId][docPropLabel])
                    else:
                        propList.append("")
                print(i)               
                masterPropList.extend([[],propList])
                #i += 2                        

    #for line in masterPropList:
    #    for s in line:
    #        print(s)
    #    sys.exit()                                        
    spreadSheetId = arguments['spreadsheetId']            
    UpdateSingleRange(masterPropList, "B2", "DP COMPARE 6", spreadSheetId)            


    sys.exit()

    return (docPropLabels, masterPropList, 0, numOfPreDocs, numOfPostDocs)

    ### PANDAS BULLSHIT ###
    #prechangeDf = pd.DataFrame(prechangeProps.values())
    #postchangeDf = pd.DataFrame(postchangeProps.values())
    #masterPropDf = pd.concat([prechangeDf, postchangeDf], ignore_index=True)

    #pd.set_option('display.max_rows', 500)
    #pd.set_option('display.max_columns', 500)
    #pd.set_option('display.width', 1000)
    #masterPropDf = masterPropDf.sort_values(by=['9999_MASTER_KEY_9999'])
    #print(masterPropDf.columns)
    ### PANDAS BULLSHIT ###

def CreateCompareTab(masterPropDf):

    #rowCount = ((len(prechangeProps) + len(postchangeProps)) * 2) + 2
    addSheetResponse = SendUpdateRequests(service, AddCompareSheet(len(masterPropDf.index)+4))
    sheetId = addSheetResponse.get('replies')[0].get('addSheet').get('properties').get('sheetId')
    sheetName = str(addSheetResponse.get('replies')[0].get('addSheet').get('properties').get('title'))

    ws = gc.open(sheetName).worksheet(sheetId)
    existing = gd.get_as_dataframe(ws)
    updated = existing.append(masterPropDf)
    gd.set_with_dataframe(ws, updated)

    sys.exit()



    UpdateSingleRange([docPropLabels], "B2", sheetName)
    rows = []
    startRowNum = 3
    currentRowNum = 3
    startColIndex = 2
    currentColIndex = 2
    #
    endColIndex = startColIndex + len(masterPropList[masterPropList.keys()[0]]) - 1 # subract 1 because we dont include docid or pre/post number
    colIndexes = range(startColIndex, endColIndex+1) #keep track of columns labels that have already turned red due to mismatch

    print ("Setting column widths...")
    print("Time Elapsed: %s" % (time.time() - startTime))
    requests = SetAutoColumnWidth(startColIndex, endColIndex)
    SendUpdateRequests(service, requests, spreadSheetId)

    #sys.exit()

    prechangeRange = []
    postchangeRange = []

    dpLabelEqual = []
    dpLabelNotEqual = []

    borderRange =[{ "sheetId": sheetId,
                    "startColumnIndex": 0,
                    "endColumnIndex": endColIndex,
                    "startRowIndex": 1,
                    "endRowIndex": 2 }]

    # NEW COLOR RANGES
    dpValuesEq = {}
    dpValuesNe = []
    dpValuesEqual = True
    dpNeCols = [] # cell range of dp labels to turn red
    dpEqRows = []
    changedDocProps = {}
    numOfChangedPairs = 0
    compareNumber = 1

    print("Number of Doc Props:", len(docPropLabels))
    for documentPair in masterPropList.values():
        # ADD DATA
        row1 = ["PRE.%06d" % compareNumber]
        row2 = ["POS.%06d" % compareNumber]
        dpRowEq = True
        for docPropValue in documentPair:
            #print(docPropValue)
            row1.append(docPropValue[0]) #prechange
            row2.append(docPropValue[1]) #postchange
            if docPropValue[0] != docPropValue[1]:
                if docPropLabels[currentColIndex-2] != "DOCUMENTID":
                    dpRowEq = False
                    # Store a list of all doc props that were changed
                    if docPropLabels[currentColIndex-2] not in changedDocProps:
                        changedDocProps[docPropLabels[currentColIndex-2]] = {"documents": ["Changed Document Pairs:", "PRE.%06d - POS.%06d" % (compareNumber, compareNumber)],
                                                                             "column": currentColIndex-1,
                                                                             "row": currentRowNum-1}
                    else:
                        changedDocProps[docPropLabels[currentColIndex-2]]["documents"].append("PRE.%06d - POS.%06d" % (compareNumber, compareNumber))
                    if currentColIndex in colIndexes:
                        dpNeCols.append({   "sheetId": sheetId,
                                            "startColumnIndex": currentColIndex - 1 ,
                                            "endColumnIndex": currentColIndex,
                                            "startRowIndex": startRowNum - 2,
                                            "endRowIndex": startRowNum-1})
                        colIndexes.remove(currentColIndex)
                    if dpValuesEqual:
                        dpValuesNe.append({ "sheetId": sheetId,
                                            "startColumnIndex": currentColIndex - 1 ,
                                            "endColumnIndex": currentColIndex,
                                            "startRowIndex": currentRowNum - 1,
                                            "endRowIndex": currentRowNum + 1})
                        dpValuesEqual = False
                    elif not dpValuesEqual:
                        dpValuesNe[-1]["endColumnIndex"] = currentColIndex
                        dpValuesNe[-1]["endRowIndex"] = currentRowNum + 1
            else:
                dpValuesEqual = True
            currentColIndex += 1
        # used for summary statement at top of page
        if not dpRowEq:
            numOfChangedPairs += 1
        # Reset bool for each pair
        dpValuesEqual = True
        # Build our 2D Array of row data
        rows.append(row1)
        rows.append(row2)

        dpLabelEqual.append("D%d=D%d" % (currentRowNum, currentRowNum+1))
        dpLabelNotEqual.append("D%d<>D%d" % (currentRowNum, currentRowNum+1))
        borderRange.append({ "sheetId": sheetId,
                             "startColumnIndex": 0,
                             "endColumnIndex": endColIndex,
                             "startRowIndex": currentRowNum,
                             "endRowIndex": currentRowNum+1})
        #requests = AddCompFormatRule(service, requests, endColIndex, currentRowNum-1)
        #AddCompFormatRule(service, "=D%s" % str(currentRowNum), endColIndex, currentRowNum)
        #####
        # Keep track of all rows that should be hidden because there was no change seen between pre and post for that pair
        if dpRowEq:
            if len(dpEqRows) > 0: # only check previous row if we have added at least one
                if dpEqRows[-1][1] == currentRowNum-1:
                    dpEqRows[-1][1] = currentRowNum+1 # extend previous range by our current pair
                else:
                    dpEqRows.extend([[currentRowNum-1, currentRowNum+1]]) # start a new range
            else:
                dpEqRows.extend([[currentRowNum-1, currentRowNum+1]]) # add our first range
        currentRowNum += 2
        compareNumber += 1
        currentColIndex = 2 # rest col index after each pair is added to rows

    # Add rows of data to sheet
    UpdateSingleRange(rows, "A%s" % str(startRowNum), sheetName)
    print("Time Elapsed: %s" % (time.time() - startTime))
    # Set all dp cells to green to start
    print("Setting all cells to green...")
    dpValuesEq = {  "sheetId": sheetId,
                    "startColumnIndex": startColIndex + 1,
                    "endColumnIndex": endColIndex,
                    "startRowIndex": startRowNum - 1,
                    "endRowIndex": currentRowNum}
    requests = AddGreenBackground(dpValuesEq)
    SendUpdateRequests(service, requests, spreadSheetId)
    # Now change all mismatched value pairs to red
    print("Changing cells to red...")
    if len(dpValuesNe) > 0:
        requests = AddRedBackground(dpValuesNe)
        SendUpdateRequests(service, requests, spreadSheetId)
    else:
        print("No differences found between the two batchs...")

    print("Setting all labels to green or red...")
    dpEqCols = [{   "sheetId": sheetId,
                    "startColumnIndex": startColIndex + 1,
                    "endColumnIndex": endColIndex,
                    "startRowIndex": startRowNum - 2,
                    "endRowIndex": startRowNum - 1}]
    requests = AddDPLabelBackground(dpEqCols, dpNeCols)
    SendUpdateRequests(service, requests, spreadSheetId)

    print ("Adding row borders...")
    AddRowBorders(service, borderRange)

    print ("Setting font to Calibri...")
    SetFont(service, endColIndex)
    #print ("Adding conditional formatting rules for each document...")
    #requests = AddDPCompFormatRule(prechangeRange, postchangeRange)
    # Send conditional formatting requests
    #SendUpdateRequests(service, requests, spreadSheetId)
    # Send alternating colors request
    print("Adding alternating colors and batch information...")
    SendUpdateRequests(service, AddAlternatingColors())
    #changedDPIndexes = docProp
    #changedDocProps = [docPropLabels[i] for i in dpNeCols] # Get dp labels that saw a change in any pre/post pair

    AddChangedCellLink(changedDocProps, sheetId, sheetName)

    AddBatchInformation(numOfPreDocs, numOfPostDocs, misMatchCount, numOfChangedPairs - misMatchCount, changedDocProps)

    AddPairColor(dpValuesNe, dpValuesEq)

    print("Hiding rows that saw no change from pre to post...")
    HideNoChangeRows(dpEqRows)

    #print("Adding summary information...")
    #summaryStatement = "Prechange Document Count: %s | Postchange Document Count: %s | Number of Mismatched Documents: %s | Number of Pairs with Change: %s" \
    #                    % (numOfPreDocs, numOfPostDocs, misMatchCount, numOfChangedPairs - misMatchCount)
    #UpdateSingleRange([[summaryStatement, ""]], "C1")



    print("Time Elapsed: %s" % (time.time() - startTime))
    print("Mission successful...")
    sys.exit()


def CreateDPCompareTab(docPropLabels, masterPropList, misMatchCount, numOfPreDocs, numOfPostDocs, arguments):

    print("Start CreateDPCompareTab: %s" % (time.time() - startTime))

    spreadSheetId = arguments['spreadsheetId']
    rowCount = (len(masterPropList) * 2) + 2
    addSheetResponse = SendUpdateRequests(service, AddCompareSheet(rowCount, spreadSheetId), spreadSheetId)
    sheetId = addSheetResponse.get('replies')[0].get('addSheet').get('properties').get('sheetId')
    sheetName = str(addSheetResponse.get('replies')[0].get('addSheet').get('properties').get('title'))

    print("sheet id: ", sheetId)
    print("sheet name: ", sheetName)

    UpdateSingleRange([docPropLabels], "B2", sheetName, spreadSheetId)
    rows = []
    startRowNum = 3
    currentRowNum = 3
    startColIndex = 2
    currentColIndex = 2
    #
    endColIndex = startColIndex + len(masterPropList[list(masterPropList.keys())[0]]) - 1 # subract 1 because we dont include docid or pre/post number
    colIndexes = range(startColIndex, endColIndex+1) #keep track of columns labels that have already turned red due to mismatch

    print ("Setting column widths...")
    print("Time Elapsed: %s" % (time.time() - startTime))
    requests = SetAutoColumnWidth(startColIndex, endColIndex, sheetId)
    SendUpdateRequests(service, requests, spreadSheetId)

    #sys.exit()

    prechangeRange = []
    postchangeRange = []

    dpLabelEqual = []
    dpLabelNotEqual = []

    borderRange =[{ "sheetId": sheetId,
                    "startColumnIndex": 0,
                    "endColumnIndex": endColIndex,
                    "startRowIndex": 1,
                    "endRowIndex": 2 }]

    # NEW COLOR RANGES
    dpValuesEq = {}
    dpValuesNe = []
    dpValuesEqual = True
    dpNeCols = [] # cell range of dp labels to turn red
    dpEqRows = []
    changedDocProps = {}
    numOfChangedPairs = 0
    compareNumber = 1

    print("Number of Doc Props:", len(docPropLabels))
    for documentPair in masterPropList.values():
        # ADD DATA
        row1 = ["PRE.%06d" % compareNumber]
        row2 = ["POS.%06d" % compareNumber]
        dpRowEq = True
        for docPropValue in documentPair:
            #print(docPropValue)
            row1.append(docPropValue[0]) #prechange
            row2.append(docPropValue[1]) #postchange
            if docPropValue[0] != docPropValue[1]:
                if docPropLabels[currentColIndex-2] != "DOCUMENTID":
                    dpRowEq = False
                    # Store a list of all doc props that were changed
                    if docPropLabels[currentColIndex-2] not in changedDocProps:
                        changedDocProps[docPropLabels[currentColIndex-2]] = {"documents": ["Changed Document Pairs:", "PRE.%06d - POS.%06d" % (compareNumber, compareNumber)],
                                                                             "column": currentColIndex-1,
                                                                             "row": currentRowNum-1}
                    else:
                        changedDocProps[docPropLabels[currentColIndex-2]]["documents"].append("PRE.%06d - POS.%06d" % (compareNumber, compareNumber))
                    if currentColIndex in colIndexes:
                        dpNeCols.append({   "sheetId": sheetId,
                                            "startColumnIndex": currentColIndex - 1 ,
                                            "endColumnIndex": currentColIndex,
                                            "startRowIndex": startRowNum - 2,
                                            "endRowIndex": startRowNum-1})
                        colIndexes.remove(currentColIndex)
                    if dpValuesEqual:
                        dpValuesNe.append({ "sheetId": sheetId,
                                            "startColumnIndex": currentColIndex - 1 ,
                                            "endColumnIndex": currentColIndex,
                                            "startRowIndex": currentRowNum - 1,
                                            "endRowIndex": currentRowNum + 1})
                        dpValuesEqual = False
                    elif not dpValuesEqual:
                        dpValuesNe[-1]["endColumnIndex"] = currentColIndex
                        dpValuesNe[-1]["endRowIndex"] = currentRowNum + 1
            else:
                dpValuesEqual = True
            currentColIndex += 1
        # used for summary statement at top of page
        if not dpRowEq:
            numOfChangedPairs += 1
        # Reset bool for each pair
        dpValuesEqual = True
        # Build our 2D Array of row data
        rows.append(row1)
        rows.append(row2)

        dpLabelEqual.append("D%d=D%d" % (currentRowNum, currentRowNum+1))
        dpLabelNotEqual.append("D%d<>D%d" % (currentRowNum, currentRowNum+1))
        borderRange.append({ "sheetId": sheetId,
                             "startColumnIndex": 0,
                             "endColumnIndex": endColIndex,
                             "startRowIndex": currentRowNum,
                             "endRowIndex": currentRowNum+1})
        #requests = AddCompFormatRule(service, requests, endColIndex, currentRowNum-1)
        #AddCompFormatRule(service, "=D%s" % str(currentRowNum), endColIndex, currentRowNum)
        #####
        # Keep track of all rows that should be hidden because there was no change seen between pre and post for that pair
        if dpRowEq:
            if len(dpEqRows) > 0: # only check previous row if we have added at least one
                if dpEqRows[-1][1] == currentRowNum-1:
                    dpEqRows[-1][1] = currentRowNum+1 # extend previous range by our current pair
                else:
                    dpEqRows.extend([[currentRowNum-1, currentRowNum+1]]) # start a new range
            else:
                dpEqRows.extend([[currentRowNum-1, currentRowNum+1]]) # add our first range
        currentRowNum += 2
        compareNumber += 1
        currentColIndex = 2 # rest col index after each pair is added to rows

    # Add rows of data to sheet
    UpdateSingleRange(rows, "A%s" % str(startRowNum), sheetName, spreadSheetId)
    print("Time Elapsed: %s" % (time.time() - startTime))
    # Set all dp cells to green to start
    print("Setting all cells to green...")
    dpValuesEq = {  "sheetId": sheetId,
                    "startColumnIndex": startColIndex + 1,
                    "endColumnIndex": endColIndex,
                    "startRowIndex": startRowNum - 1,
                    "endRowIndex": currentRowNum}
    requests = AddGreenBackground(dpValuesEq)
    SendUpdateRequests(service, requests, spreadSheetId)
    # Now change all mismatched value pairs to red
    print("Changing cells to red...")
    if len(dpValuesNe) > 0:
        requests = AddRedBackground(dpValuesNe)
        SendUpdateRequests(service, requests, spreadSheetId)
    else:
        print("No differences found between the two batchs...")

    print("Setting all labels to green or red...")
    dpEqCols = [{   "sheetId": sheetId,
                    "startColumnIndex": startColIndex + 1,
                    "endColumnIndex": endColIndex,
                    "startRowIndex": startRowNum - 2,
                    "endRowIndex": startRowNum - 1}]
    requests = AddDPLabelBackground(dpEqCols, dpNeCols)
    SendUpdateRequests(service, requests, spreadSheetId)

    print ("Adding row borders...")
    requests = AddRowBorders(service, borderRange)
    SendUpdateRequests(service, requests, spreadSheetId)

    print ("Setting font to Calibri...")
    requests = SetFont(service, endColIndex, sheetId)
    SendUpdateRequests(service, requests, spreadSheetId)
    #print ("Adding conditional formatting rules for each document...")
    #requests = AddDPCompFormatRule(prechangeRange, postchangeRange)
    # Send conditional formatting requests
    #SendUpdateRequests(service, requests, spreadSheetId)
    # Send alternating colors request
    print("Adding alternating colors and batch information...")
    SendUpdateRequests(service, AddAlternatingColors(sheetId), spreadSheetId)
    #changedDPIndexes = docProp
    #changedDocProps = [docPropLabels[i] for i in dpNeCols] # Get dp labels that saw a change in any pre/post pair

    AddChangedCellLink(changedDocProps, sheetId, sheetName, arguments)

    requests = AddBatchInformation(numOfPreDocs, numOfPostDocs, misMatchCount, numOfChangedPairs - misMatchCount, changedDocProps, sheetId, sheetName, arguments)
    SendUpdateRequests(service, requests, spreadSheetId)

    requests = AddPairColor(dpValuesNe, dpValuesEq, sheetId)
    SendUpdateRequests(service, requests, spreadSheetId)

    print("Hiding rows that saw no change from pre to post...")
    requests = HideNoChangeRows(dpEqRows, sheetId)
    if requests != []:
        SendUpdateRequests(service, requests, spreadSheetId)

    #print("Adding summary information...")
    #summaryStatement = "Prechange Document Count: %s | Postchange Document Count: %s | Number of Mismatched Documents: %s | Number of Pairs with Change: %s" \
    #                    % (numOfPreDocs, numOfPostDocs, misMatchCount, numOfChangedPairs - misMatchCount)
    #UpdateSingleRange([[summaryStatement, ""]], "C1")



    print("Time Elapsed: %s" % (time.time() - startTime))
    print("Mission successful...")
    sys.exit()

def AddPairColor(dpValuesNe, dpValuesEq, sheetId):
    requests = [
    {
      "updateDimensionProperties": {
        "range": {
          "sheetId": sheetId,
          "dimension": "COLUMNS",
          "startIndex": 2,
          "endIndex": 3
        },
        "properties": {
          "pixelSize": 5
        },
        "fields": "pixelSize"
      }
    },
    {
      "repeatCell": {
        "range": {
          "sheetId": sheetId,
          "startColumnIndex": 2,
          "endColumnIndex": 3,
          "startRowIndex": 0,
          "endRowIndex": 2
        },
        "cell": {
          "userEnteredFormat": {
            "backgroundColor": {
              "blue": 0.8,  # Gray
              "green": 0.8,
              "red": 0.8,
            },
          }
        },
        "fields": "userEnteredFormat(backgroundColor)"
      }
    }]

    requests.append(
    {
      "repeatCell": {
        "range": {
          "sheetId": sheetId,
          "startColumnIndex": 2,
          "endColumnIndex": 3,
          "startRowIndex": dpValuesEq["startRowIndex"],
          "endRowIndex": dpValuesEq["endRowIndex"]
        },
        "cell": {
          "userEnteredFormat": {
            "backgroundColor": {
              "blue": 0.588,  # Dark green if equal
              "green": 0.815,
              "red": 0.568,
            },
          }
        },
        "fields": "userEnteredFormat(backgroundColor)"
      }
    })

    for pair in dpValuesNe:
        requests.append(
        {
          "repeatCell": {
            "range": {
              "sheetId": sheetId,
              "startColumnIndex": 2,
              "endColumnIndex": 3,
              "startRowIndex": pair["startRowIndex"],
              "endRowIndex": pair["endRowIndex"]
            },
            "cell": {
              "userEnteredFormat": {
                "backgroundColor": {
                  "blue": 0.44,  # Dark red if not equal
                  "green": 0.44,
                  "red": 0.874,
                },
              }
            },
            "fields": "userEnteredFormat(backgroundColor)"
          }
        })
    #SendUpdateRequests(service, requests, spreadSheetId)
    return requests


def AddGreenBackground(dpValuesEq):
    requests = [
    {
      "repeatCell": {
        "range": dpValuesEq,
        "cell": {
          "userEnteredFormat": {
            "backgroundColor": {
              "blue": 0.827451,  # Light green if equal
              "green": 0.91764706,
              "red": 0.8509804,
            },
          }
        },
        "fields": "userEnteredFormat(backgroundColor)"
      }
    }]
    return requests

def AddRedBackground(dpValuesNe):
    requests = []
    for cellRange in dpValuesNe:
        requests.append(
        {
          "repeatCell": {
            "range": cellRange,
            "cell": {
              "userEnteredFormat": {
                "backgroundColor": {
                  "blue": 0.8,  # Light red if not equal
                  "green": 0.8,
                  "red": 0.95686175,
                },
              }
            },
            "fields": "userEnteredFormat(backgroundColor)"
          }
        })
    return requests

def AddDPLabelBackground(dpEqCols, dpNeCols):
    requests = []
    for cellRange in dpEqCols:
        requests.append(
        {
          "repeatCell": {
            "range": cellRange,
            "cell": {
              "userEnteredFormat": {
                "backgroundColor": {
                  "blue": 0.588,  # Dark green if equal
                  "green": 0.815,
                  "red": 0.568,
                },
              }
            },
            "fields": "userEnteredFormat(backgroundColor)"
          }
        })
    for cellRange in dpNeCols:
        requests.append(
        {
          "repeatCell": {
            "range": cellRange,
            "cell": {
              "userEnteredFormat": {
                "backgroundColor": {
                  "blue": 0.44,  # Dark red if not equal
                  "green": 0.44,
                  "red": 0.874,
                },
              }
            },
            "fields": "userEnteredFormat(backgroundColor)"
          }
        })
    return requests

def AddAlternatingColors(sheetId):
    requests = [{
       'addBanding':{
          'bandedRange':{
             'range':{
                'sheetId':sheetId,
                'startRowIndex':2,
                'startColumnIndex':0,
                'endColumnIndex':2,
             },
             'rowProperties':{
                'firstBandColor':{
                   'red':1,
                   'green':.89,
                   'blue':.74,
                },
                'secondBandColor':{
                   'red':.776,
                   'green':.905,
                   'blue':1,
                }
             },
          },
       },
    },
    {
       'updateSheetProperties':{
          'properties':{
             'sheetId':sheetId,
             'gridProperties':{
                'frozenRowCount':2
             }
          },
          'fields':'gridProperties.frozenRowCount',
       }
    },
    {
       'updateSheetProperties':{
          'properties':{
             'sheetId':sheetId,
             'gridProperties':{
                'frozenColumnCount':3
             }
          },
          'fields':'gridProperties.frozenColumnCount',
       }
    },]
    return requests

def SetFont(service, endColumnIndex, sheetId):
    requests =  [
    {
      "repeatCell": {
        "range": {
          "sheetId": sheetId,
          "startColumnIndex": 0,
          "endColumnIndex": endColumnIndex
        },
        "cell": {
          "userEnteredFormat": {
            "textFormat": {
              "fontFamily": "Calibri",
            }
          }
        },
        "fields": "userEnteredFormat(textFormat)"
      }
    },
    {
      "repeatCell": {
        "range": {
          "sheetId": sheetId,
          "startColumnIndex": 0,
          "endColumnIndex": 2,
          "startRowIndex": 1,
          "endRowIndex": 2
        },
        "cell": {
          "userEnteredFormat": {
          "backgroundColor": {
              "red": 0.8,
              "green": 0.8,
              "blue": 0.8
            },
          }
        },
        "fields": "userEnteredFormat(backgroundColor)"
      }
    },
    {
      "repeatCell": {
        "range": {
          "sheetId": sheetId,
          "startRowIndex": 0,
          "endRowIndex": 2
        },
        "cell": {
          "userEnteredFormat": {
            "textFormat": {
              "fontFamily": "Calibri",
              "bold": True,
            }
          }
        },
        "fields": "userEnteredFormat(textFormat)"
      }
    },
    ]
    return requests
    #SendUpdateRequests(service, requests, spreadSheetId)

def AddRowBorders(service, borderRange):
    requests = []
    for ranges in borderRange:
        requests.append({'updateBorders': {'range': ranges, 'bottom': {"style": "SOLID",
                                                                       "width": 2,
                                                                       "color": {'red': 0,
                                                                               'green': 0,
                                                                               'blue': 0,}}}})
    return requests

def AddChangedCellLink(changedDocProps, sheetId, sheetName, arguments):
    temp = arguments['spreadsheetURL'][:arguments['spreadsheetURL'].rfind("edit#gid=")]
    url = temp + "edit#gid=" + str(sheetId)
    for changedProp in changedDocProps:
        if changedProp not in ("", "(none)"):
            firstChangeA1 = GetA1Notation(changedDocProps[changedProp]["column"], changedDocProps[changedProp]["row"])
            colLabelA1 = GetA1Notation(changedDocProps[changedProp]["column"], 0)
            value = "=HYPERLINK(\"%s&range=%s\", \"%s\")" % (url, firstChangeA1, "Find Change")
            UpdateSingleRange([[value,]], colLabelA1, sheetName, arguments['spreadsheetId'], value_input_option="USER_ENTERED")

def GetA1Notation(columnIndex, rowIndex):
    quot, rem = divmod(columnIndex, 26)
    return((chr(quot-1 + ord('A')) if quot else '') +
           (chr(rem + ord('A')) + str(rowIndex+1)))


def AddBatchInformation(numOfPreDocs, numOfPostDocs, misMatchCount, numOfChangedPairs, changedDocProps, sheetId, sheetName, arguments):
    note = "Prechange Document Count:\n    %s\n"    \
           "Postchange Document Count:\n    %s\n"   \
           "Mismatched Document Count:\n    %s\n"   \
           "Changed Pre/Post Pair Count:\n    %s\n" \
           "List of changed Doc Props:"             \
           % (numOfPreDocs, numOfPostDocs, misMatchCount, numOfChangedPairs)
    if len(changedDocProps) == 0:
        note += "\n    (none)"
    for changedProp in changedDocProps:
        note += "\n    %s" % changedProp
    requests = [
    {
      "repeatCell": {
        "range": {
          "sheetId": sheetId,
          "startColumnIndex": 0,
          "endColumnIndex": 1,
          "startRowIndex": 0,
          "endRowIndex": 1
          },
        "cell": {
          "userEnteredFormat": {
            "backgroundColor": {
              "blue": 0.776,  # Prechange
              "green": 0.905,
              "red": 1,
            },
          },
        },
        "fields": "userEnteredFormat(backgroundColor)"
      }
    },
    {
      "repeatCell": {
        "range": {
          "sheetId": sheetId,
          "startColumnIndex": 1,
          "endColumnIndex": 2,
          "startRowIndex": 0,
          "endRowIndex": 1
          },
        "cell": {
          "userEnteredFormat": {
            "backgroundColor": {
              "blue": 1,  # Postchange
              "green": 0.89,
              "red": 0.74,
            },
          }
        },
        "fields": "userEnteredFormat(backgroundColor)"
      }
    },
    {
      "repeatCell": {
        "range": {
          "sheetId": sheetId,
          "startColumnIndex": 3,
          "endColumnIndex": 4,
          "startRowIndex": 0,
          "endRowIndex": 1
          },
        "cell": {
          "note" : note
        },
        "fields": "note"
      }
    },]
    for changedProp in changedDocProps.values():
        if changedProp != "":
            requests.append(    {
                                  "repeatCell": {
                                    "range": {
                                      "sheetId": sheetId,
                                      "startColumnIndex": changedProp["column"],
                                      "endColumnIndex": changedProp["column"]+1,
                                      "startRowIndex": 1,
                                      "endRowIndex": 2
                                      },
                                    "cell": {
                                      "note" : "\n".join(changedProp["documents"]),
                                      "userEnteredFormat": {
                                        "textFormat": {
                                          "foregroundColor": {
                                            "red":   0,
                                            "green": 0,
                                            "blue":  0,
                                          },
                                          "underline": False,
                                          "fontFamily": "Calibri",
                                          "bold": True,
                                        },
                                      },
                                    },
                                    "fields": "note, userEnteredFormat(textFormat)"
                                  }
                                })
    #SendUpdateRequests(service, requests, spreadSheetId)

    prePost = ("Pre:  " + str(arguments['preId']), "Post:  " + str(arguments['postId']), "", "BATCH COMPARE STATS")
    csrId = ("CSR ID:  " + str(arguments['custId']), "DOCUMENTID")
    UpdateSingleRange([prePost], "A1", sheetName, arguments['spreadsheetId'])
    UpdateSingleRange([csrId], "A2", sheetName, arguments['spreadsheetId'])

    return requests

def HideNoChangeRows(dpEqRows, sheetId):
    requests = []
    for startIndex, endIndex in dpEqRows:
        requests.append({
          'updateDimensionProperties': {
            "range": {
              "sheetId": sheetId,
              "dimension": 'ROWS',
              "startIndex": startIndex,
              "endIndex": endIndex,
            },
            "properties": {
              "hiddenByUser": True,
            },
            "fields": 'hiddenByUser',
        }})
    #if requests != []:
    #    SendUpdateRequests(service, requests, spreadSheetId)
    return requests

def AddCompareSheet(rowCount, spreadsheetId):
    print("Start: Add new sheet to google doc")
    request = service.spreadsheets().get(spreadsheetId=spreadsheetId, fields="sheets.properties")
    response = request.execute()
    sheetNumbers = [0]
    for sheet in response.get('sheets'):
        sheetName = str(sheet.get('properties').get('title'))
        if "DP COMPARE" in sheetName:
            if sheetName.split(" ")[-1].isdigit():
                sheetNumbers.append(int(sheetName.split(" ")[-1]))
    newSheetNumber = max(sheetNumbers) + 1
    title = "DP COMPARE %d" % newSheetNumber
    print("New sheet name: %s" % title)
    # Alternate colors of added sheets
    red = 0.55
    green = 1.0
    blue = 0.64
    if max(sheetNumbers) % 2 == 0:
        red = 0.22
        green = 0.19
        blue = 1.0

    requests = [{"addSheet": {"properties": {"title": title,
                                             "gridProperties": {"rowCount": rowCount,
                                                                "columnCount": 25},
                                                                "tabColor": { "red": red,
                                                                              "green": green,
                                                                              "blue": blue}}}}]
    return requests

def SetColumnWidth(startIndex, endIndex, sheetId):
    requests = [{"updateDimensionProperties":{"range":{ "sheetId": sheetId,
                                                        "dimension": "COLUMNS",
                                                        "startIndex": startIndex,
                                                        "endIndex": endIndex},
                                                        "properties":{  "pixelSize": 160},
                                                                        "fields": "pixelSize"}}]
    return requests

def SetAutoColumnWidth(startIndex, endIndex, sheetId):
    requests = [{"autoResizeDimensions":{"dimensions":{"sheetId": sheetId,
                                                      "dimension": "COLUMNS",
                                                      "startIndex": startIndex,
                                                      "endIndex": endIndex}}}]
    return requests

def SendUpdateRequests(service, requests, spreadsheetId):
    body = {'requests': requests}
    #print(body)
    #print(spreadsheetId)
    response = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheetId,body=body).execute()
    return response



#####################################################
#####################################################
#####################################################
################## THINGS I HATE ####################
# DONE, STILL NEEDS WORK- All of these stupid globals
# DONE, NEEDS REVIEW- No main function
# JSON could be moved into a separate file
# I loop through the docprops too many times which is slow AF
# The mongo query can take upwards of 30 seconds, not sure if this can be improved or not
# DONE(by default all pairs that saw no change are now hidden)-If there are a lot of documents to compare and a column header is red,
#       it can be hard to find which pair has the diff
# How do we identify duplicated documents, sometimes only 1 property is different between the two.. usually routing but could be some other prop
# DONE- Since coversheets create their own record in fsidocprops, we need a way to differentiate and ignore these records.
#       The only way to do this is to include sql queries for things like FFDID or BTROUTE.
# DONE- A lot of 'nice to have properties' are not saved in fsidocprops: routing, template/ffdid, page count, ect
# DONE, WHO CARES- COL properties are not included in this compare.
# DONE- Compare Tab does not include the batch numbers or customer name, add some header info
# DONE- I am not using any version control software
# ORIGINAL_BATCHID is not captured in Mongo, so bullpenned docs are always under thier original batch
# DONE- DOCUMENTID can be wrong if the mastkey fails to be unique, right now this should cause the scrip to exit
# Add drop down of links to all pairs that saw change - GOOGLE DOESNT SUPPORT THIS, FIND A WORKAROUND?
# Currently, I combine all the doc prop labels into one list from both pre and post batches, later on I loop through
#   this list and check for the existance of that property in each batch.  Rather than looping through the combined list
#   for both pre and post batches, I should create two additional lists or add an indicator to my combined list to specify
#   which batch has that doc prop.  From there I will loop through their respective lists rather than the combined list to save time.
# Add exception handling for duplicate master keys, maybe a section at the bottom of the sheet for unmatched documents?


def run(argv):
    if len(argv) != 4:
        print("Command line arguments not given, using values hardcoded "      \
              "within run() function...")

        spreadsheetURL = 'https://docs.google.com/spreadsheets/d/1-SWPPRg2i2IsTgUA-4BvpEkMyE1TBZUvmEHZw1zpWo4/edit?usp=drive_web&ouid=116956695434029002425'
        spreadsheetId = spreadsheetURL[:spreadsheetURL.rfind("/")]
        spreadsheetId = spreadsheetId[spreadsheetId.rfind("/")+1:]

        arguments = {"custId"           : 961,
                     "preId"            : 13847185,
                     "postId"           : 13847315,
                     "spreadsheetURL"   : spreadsheetURL,
                     "spreadsheetId"    : spreadsheetId}
        pprint(arguments)
    else:
        spreadsheetId = argv[3][:argv[3].rfind("/")]
        spreadsheetId = spreadsheetId[spreadsheetId.rfind("/")+1:]
        arguments = {"custId"           : int(argv[0]),
                     "preId"            : int(argv[1]),
                     "postId"           : int(argv[2]),
                     "spreadsheetURL"   : argv[3],
                     "spreadsheetId"    : spreadsheetId}
        print("Proceeding with the following command line arguments...")
        pprint(arguments)

    #sys.exit()

    # connect to Sql Server, TODO this should be a cmd line arg
    # setting as false, not all users have windows auth, need to add generic
    # gmcuser creds
    bConnToSqlServer = False
    if bConnToSqlServer:
        sqlServerCursor = InitSqlServerClient()
    # Get list of Coversheet FFDIds
    im_sqlClient, prod_sqlClient = InitSQLClient()
    coversheetDocIds = GetCoversheetDocIds(im_sqlClient, prod_sqlClient, arguments )

    # Get ffdid, routing and pagecount from fsidocument, returns two lists of dicts, a prechange and postchange
    fsiDocumentInfo = GetFSIDocumnetInfo(im_sqlClient, arguments)

    # Init mongo client, returns the fsidocprops collection to query against
    im_fsidocprops, prod_fsidocprops = InitMongoClient(queryProd=False)

    # Get list of doc prop names from prechange and postchange excluding coversheets
    prePostDocProps = GetDocProps(im_fsidocprops, coversheetDocIds, arguments)
    #prePostDocProps = QueryMongo(coversheetDocIds, fsidocprops, arguments)

    # Merge pre and post batches, returns list = [docPropLabels, masterPropList, misMatchCount, numOfPreDocs, numOfPostDocs]
    mergedData = MergeBatchData(prePostDocProps[0], prePostDocProps[1], fsiDocumentInfo[0], fsiDocumentInfo[1])
    #mergedData = MergeToDataFrame(prePostDocProps[0], prePostDocProps[1], fsiDocumentInfo[0], fsiDocumentInfo[1], arguments)

    # Add all information to our google sheet and format cells accordingly
    CreateDPCompareTab(mergedData[0], mergedData[1], mergedData[2], mergedData[3], mergedData[4], arguments)


# Used to calc processing time
startTime = time.time()

# Authorize Google Sheets API credentials and build service, service obj returned
service = GoogleAPIAuthorization()

# List of properties that we never want to include in our compare
ignoreTheseDocProps = ('FILEDATE', 'FILE_PREFIX', 'XML_DATA', 'BT_PRINT_FILE_NAME', 'BILLING_ADDRESS_BEG1', 'BILLING_ADDRESS_BEG2',
               'BILLING_ADDRESS_END1', 'BILLING_ADDRESS_END2', 'BILLING_ADDRESS_ZIP4', 'BILLING_ADDRESS_ZIP5', 'BILLING_ADDRESS_CITY',
               'BILLING_ADDRESS_STATE', 'ROWIMG', 'JOB_ID')


if __name__ == "__main__":
    run(sys.argv[1:])
