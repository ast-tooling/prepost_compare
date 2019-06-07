from __future__ import print_function
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
# Mongo client
import pymongo
from pymongo import MongoClient
from pymongo import MongoReplicaSetClient
from pprint import pprint
# User Info
import os
import json
from itertools import tee
from collections import OrderedDict


def GoogleAPIAuthorization():
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
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
    return creds        

def CreateExample(service):
    spreadsheet_body = {
        # TODO: Add desired entries to the request body.
    }
    request = service.spreadsheets().create(body=spreadsheet_body)
    response = request.execute()
    #pprint(response)
    return response

# Get range of cells which includes all metadata
def GetExample():
    global spreadsheetId
    #ranges = ["'DP Compare'!B6:B7"]
    ranges = ["D12"]
    include_grid_data = True
    request = service.spreadsheets().get(spreadsheetId=spreadsheetId, ranges=ranges, includeGridData=include_grid_data)
    response = request.execute()
    #pprint(response)
    
# Get values from a specified range
def GetRange():
    global spreadsheetId
    #spreadsheet_id = "1M8vB_PFEqXVnVSQt9X92yLNPG-x_ETtofN7ZeB9WAgU"
    range_names = ["D10:E10"]
    result = service.spreadsheets().values().batchGet(
        spreadsheetId=spreadsheetId, ranges=range_names).execute()
    #print('{0} ranges retrieved.'.format(result.get('valueRanges')))

# Write a single range of values out
def UpdateSingleRange(values, startPos):
    global spreadsheetId #this has to be turned into an input param
    value_input_option = "RAW" #input raw string data, no formulas, dates, currency, ect.
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
            body = {'values': values[i:i + rowsPerUpdate]}
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheetId, range=startPos,
            valueInputOption=value_input_option, body=body).execute()
        print('{0} cells updated.'.format(result.get('updatedCells')))
        # update starting position
        startCol = startPos.split("!")[1] 
        startPos = sheetName + '!' + filter(str.isalpha, startCol) + str(int(filter(str.isdigit, startCol)) + rowsPerUpdate)

#########################
def InitMongoClient():
    ###############################
    # START: GET USER CREDENTIALS #
    userName = os.getenv('username')
    userPassword = ''
    roboPath = "C:\\Users\\%s\\.3T\\robo-3t\\1.2.1\\robo3t.json" % userName

    print ("Validating user credentials...")
    with open(roboPath) as json_file:
        connectionData = json.load(json_file)
        for connection in connectionData['connections']:
            if connection['serverHost'] == "ssnj-immongodb01":
                for cred in connection['credentials']:
                    userName = cred['userName']
                    userPassword = cred['userPassword']
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
    fsidocprops = imsMongoClient.docpropsdb.fsidocprops
    return fsidocprops
    # END: CONNECT TO MONGO CLIENT #
    ################################
def GetDocProps():
    global prechangeProps, postchangeProps, preId, postId, custId, startTime
    fsidocprops = InitMongoClient()
    # START: QUERY FOR DP #
    #######################
    #preId = int(app.getEntry('ePrechangeId'))
    #postId = int(app.getEntry('ePostchangeId'))
    #custId = int(app.getEntry('eCustId'))
    print("Time Elapsed: %s" % (time.time() - startTime))
    print("Querying for prechange and postchange doc props...")

    prechangeProps = list(fsidocprops.find({'batchId': preId, 'customerId': custId}))
    postchangeProps = list(fsidocprops.find({'batchId': postId, 'customerId': custId}))#,{'_id':0, 'properties':1}))
    #print(prechangeProps)
    # END: QUERY FOR DP #
    #####################
    print("Query finished... CustomerId: %s  Prechange: %s  Postchange: %s" % (str(custId), str(preId), str(postId)))
    print("Time Elapsed: %s" % (time.time() - startTime))


    #################
    # TEST FUNCTION #
    MergeBatchData(prechangeProps, postchangeProps)

def QueryMongo():
    global prechangeProps, postchangeProps, preId, postId, custId, startTime
    fsidocprops = InitMongoClient()
    # START: QUERY FOR DP #
    #######################
    #preId = int(app.getEntry('ePrechangeId'))
    #postId = int(app.getEntry('ePostchangeId'))
    #custId = int(app.getEntry('eCustId'))
    print("Time Elapsed: %s" % (time.time() - startTime))
    print("Querying for prechange and postchange doc props...")

    prechangeProps = pd.DataFrame(list(fsidocprops.find({'batchId': preId, 'customerId': custId})))
    print(prechangeProps.size)
    print(prechangeProps.columns)
    #postchangeProps = list(fsidocprops.find({'batchId': postId, 'customerId': custId}))#,{'_id':0, 'properties':1}))
    #print(prechangeProps)
    # END: QUERY FOR DP #
    #####################
    print("Query finished... CustomerId: %s  Prechange: %s  Postchange: %s" % (str(custId), str(preId), str(postId)))
    print("Time Elapsed: %s" % (time.time() - startTime))

    sys.exit()
    #################
    # TEST FUNCTION #
    MergeBatchData(prechangeProps, postchangeProps)    

def MergeBatchData(prechangeProps, postchangeProps):
    print("Starting MergerBatchData...")
    docPropLabels = []
    # Add all doc props from our prechange and postchange batches to a list of doc prop names
    for batch in (prechangeProps, postchangeProps):
        for document in batch:
            for prop in document.get('properties'):
                docPropName = prop.get('k')
                if docPropName: # Do not add columnar properties or special biscuit generated properties.. XML_DATA was causing a failure
                    if not docPropName.endswith("_COL") and docPropName not in ('FILEDATE', 'FILENAME', 'FILE_PREFIX', 'XML_DATA', 'BT_PRINT_FILE_NAME', ''):
                        if docPropName not in docPropLabels:
                            docPropLabels.append(str(docPropName))

    # Sort the labels and add DOCUMENTID, ACCOUNT_NUMBER and INVOICE_NUMBER to the front\
    docPropLabels.sort()
    props = ["INVOICE_NUMBER", "ACCOUNT_NUMBER"]
    for prop in props:
        if prop in docPropLabels:
            docPropLabels.remove(prop)
            docPropLabels.insert(0, prop)
    docPropLabels.insert(0, "DOCUMENTID")

    # masterPropList will contain our final structure of pre and post doc props with a masterKey
    # masterKey is currently ''.join(ACCOUNT_NUMBER INVOICE_NUMBER) but should optionally be user defined
    # {ACCOUNT_NUMBERINVOICE_NUMBER: [prechangePropValue, postchangePropValue], [prechangePropValue, postchangePropValue], ...}
    masterPropList = {}
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
    for document in prechangeProps:        
        # Get master key before starting
        masterKey = ['','']
        #print(document)
        #sys.exit()     
        for prop in document.get('properties'):
            if prop.get('k') == "ACCOUNT_NUMBER":
                masterKey[0] = prop.get('v')
            elif prop.get('k') == "INVOICE_NUMBER":
                masterKey[1] = prop.get('v')
        #print(masterKey)       
        masterKey = ''.join(masterKey)
        count += 1

    
        # Exit if we were not able to find either account number or invoice number
        if masterKey == '':
            print("Not able to find either account number or invoice number in the prechange batch")
            print(str(count))
            sys.exit()         

        #print (docProps)
        for docPropLabel in docPropLabels:
            if docPropLabel == "DOCUMENTID":
                masterPropList[masterKey] = [[str(document.get('documentId')), '']]
            else:    
                tempPropValues = ['', '']
                for prop in document.get('properties'):
                    propName = prop.get('k')
                    if propName == docPropLabel:
                        tempPropValues[0] = prop.get('v').replace('<BR>', '\n')[:5000] #google sheets limits cell data to 5000 chars
                        break
                masterPropList[masterKey].append(tempPropValues)          
        # END PRECHANGE PROPS    

    # START POSTCHANGE PROPS
    for document in postchangeProps:
        # Get master key before starting
        masterKey = ['','']        
        for prop in document.get('properties'):
            if prop.get('k') == "ACCOUNT_NUMBER":
                masterKey[0] = prop.get('v')
            elif prop.get('k') == "INVOICE_NUMBER":
                masterKey[1] = prop.get('v')
        masterKey = ''.join(masterKey)       
        # Exit if we were not able to find either account number or invoice number
        if masterKey == '':
            print("Not able to find either account number or invoice number in the postchange batch")
            sys.exit()
        elif not masterKey in masterPropList:
            print("Postchange masterkey not found in prechange masterkeylist, adding mismatched key.")
            masterPropList[masterKey] = []
        else:    
            for i, docPropLabel in enumerate(docPropLabels):
                if docPropLabel == "DOCUMENTID":
                    masterPropList[masterKey][i][1] = str(document.get('documentId'))
                else:    
                    for prop in document.get('properties'):
                        propName = prop.get('k')
                        if propName == docPropLabel:
                            masterPropList[masterKey][i][1] = prop.get('v').replace('<BR>', '\n')[:5000] #google sheets limits cell data to 5000 chars
                            break                                    
    print("Time Elapsed: %s" % (time.time() - startTime))   
    CreateDPCompareTab(docPropLabels, masterPropList)

def CreateDPCompareTab(docPropLabels, masterPropList):
    global sheetId, sheetName, service
    rowCount = (len(masterPropList) * 2) + 2
    addSheetResponse = SendUpdateRequests(service, AddCompareSheet(rowCount))
    sheetId = addSheetResponse.get('replies')[0].get('addSheet').get('properties').get('sheetId')
    sheetName = str(addSheetResponse.get('replies')[0].get('addSheet').get('properties').get('title'))

    UpdateSingleRange([docPropLabels], "B2")
    rows = []
    startRowNum = 3
    currentRowNum = 3
    startColIndex = 2
    currentColIndex = 2
    # 
    endColIndex = startColIndex + len(masterPropList[masterPropList.keys()[0]]) - 1 # subract 1 because we dont include docid or pre/post number
    
    print ("Setting column widths...")
    print("Time Elapsed: %s" % (time.time() - startTime))
    requests = SetAutoColumnWidth(startColIndex, endColIndex)
    SendUpdateRequests(service, requests)  
    
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

    compareNumber = 1    
    for documentPair in masterPropList.values():
        # ADD DATA
        row1 = ["PRE.%06d" % compareNumber]        
        row2 = ["POS.%06d" % compareNumber]  
        for docPropValue in documentPair:
            #print(docPropValue)
            row1.append(docPropValue[0]) #prechange
            row2.append(docPropValue[1]) #postchange
            if (docPropValue[0] != docPropValue[1]):
                if dpValuesEqual:   
                    dpValuesNe.append({ "sheetId": sheetId,
                                        "startColumnIndex": currentColIndex - 1 ,
                                        "endColumnIndex": currentColIndex,
                                        "startRowIndex": currentRowNum - 1,
                                        "endRowIndex": currentRowNum + 1})
                    dpValuesEqual == False
                elif not dpValuesEqual:
                    dpValuesNe[-1]["endColumnIndex"] = currentColIndex
                    dpValuesNe[-1]["endRowIndex"] = currentRowNum + 1
            else:
                dpValuesEqual = True
            currentColIndex += 1            
        # Build our 2D Array of row data    
        rows.append(row1)
        rows.append(row2)
            
        dpLabelEqual.append("C%d=C%d" % (currentRowNum, currentRowNum+1))
        dpLabelNotEqual.append("C%d<>C%d" % (currentRowNum, currentRowNum+1))
        borderRange.append({ "sheetId": sheetId,
                             "startColumnIndex": 0,
                             "endColumnIndex": endColIndex,
                             "startRowIndex": currentRowNum,
                             "endRowIndex": currentRowNum+1})
        #requests = AddCompFormatRule(service, requests, endColIndex, currentRowNum-1)
        #AddCompFormatRule(service, "=D%s" % str(currentRowNum), endColIndex, currentRowNum)
        #####                  
        currentRowNum += 2
        compareNumber += 1
        currentColIndex = 2 # rest col index after each pair is added to rows

    # Add rows of data to sheet       
    UpdateSingleRange(rows, "A%s" % str(startRowNum))
    print("Time Elapsed: %s" % (time.time() - startTime))
    # Set all dp cells to green to start
    print("Setting all cells to green...")
    dpValuesEq = {  "sheetId": sheetId,
                    "startColumnIndex": startColIndex,
                    "endColumnIndex": endColIndex,
                    "startRowIndex": startRowNum - 1,
                    "endRowIndex": currentRowNum}
    requests = AddGreenBackground(dpValuesEq)
    SendUpdateRequests(service, requests)
    # Now change all mismatched value pairs to red 
    print("Changing cells to red...")
    if len(dpValuesNe) > 0:
        requests = AddRedBackground(dpValuesNe)
        SendUpdateRequests(service, requests)
    else:
        print("No differences found between the two batchs...")      

    AddDPLabelCond(service, startColIndex, endColIndex, dpLabelEqual, dpLabelNotEqual)
    print ("Adding row borders...")
    AddRowBorders(service, borderRange)
    print ("Setting font to Calibri...")
    SetFont(service, endColIndex)  
    print ("Adding conditional formatting rules for each document...")
    #requests = AddDPCompFormatRule(prechangeRange, postchangeRange)    
    # Send conditional formatting requests
    #SendUpdateRequests(service, requests)
    # Send alternating colors request
    SendUpdateRequests(service, AddAlternatingColors())      
    print("Time Elapsed: %s" % (time.time() - startTime))
    sys.exit()   

def AddGreenBackground(dpValuesEq):
    requests = [
    {
      "repeatCell": {
        "range": dpValuesEq,
        "cell": {
          "userEnteredFormat": {
            "backgroundColor": {
              "blue": 0.827451,  
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
                  "blue": 0.8,  
                  "green": 0.8,
                  "red": 0.95686175,
                },
              }  
            },
            "fields": "userEnteredFormat(backgroundColor)"
          }
        })
    return requests


# THIS FUNCTION HAS BEEN ADDED TO MergeBatchData()
def GetLabels(prechangeProps, postchangeProps):
    docPropLabels = []
    # Add all doc props from our prechange batch to a list of doc prop names
    for document in prechangeProps:
        for prop in document.get('properties'):
            docPropName = prop.get('k')
            if docPropName: # Do not add columnar properties or special biscuit generated properties.. XML_DATA was causing a failure
                if not docPropName.endswith("_COL") and docPropName not in ('FILEDATE', 'FILENAME', 'FILE_PREFIX', 'XML_DATA', 'BT_PRINT_FILE_NAME', ''):
                    if docPropName not in docPropLabels:
                        docPropLabels.append(str(docPropName))
    # Now loop through our postchange batch and add all doc prop names that have not already been added                   
    for document in postchangeProps:
        for prop in document.get('properties'):
            docPropName = prop.get('k')
            if docPropName: # Do not add columnar properties or special biscuit generated properties.. XML_DATA was causing a failure
                if not docPropName.endswith("_COL") and docPropName not in ('FILEDATE', 'FILENAME', 'FILE_PREFIX', 'XML_DATA', 'BT_PRINT_FILE_NAME', ''):
                    if docPropName not in docPropLabels:
                        docPropLabels.append(str(docPropName)) 
    # add label sorting here?                                                                           
    return docPropLabels 
# THIS FUNCTION HAS BEEN ADDED TO MergeBatchData()
def GetPropValues(docPropLabels, prechangeProps, postchangeProps):
    masterPropList = []
    for i, document in enumerate(prechangeProps):
        #print(document.get('documentId'))
        docProps = OrderedDict()
        docProps["DOCUMENTID"] = [str(document.get('documentId')), str(postchangeProps[i].get('documentId'))]
        #print (docProps)
        for docProp in docPropLabels:
            if docProp != "DOCUMENTID":
                docProps[docProp] = ['', '']
        for prop in document.get('properties'):
            propName = prop.get('k')
            if propName in docPropLabels:
                #print(propName)
                docProps[propName] = [prop.get('v').replace('<BR>', '\n'), '']
                #print(docProps[prop.get('k')])
            #print(docProps)              
        for prop in postchangeProps[i].get('properties'):
            #if not prop.get('k')[-4:] == "_COL" and prop.get('k') not in ('FILEDATE', 'FILENAME', 'FILE_PREFIX'):
            propName = prop.get('k')
            if propName in docPropLabels:
                if propName in docProps:
                    docProps[propName] = [docProps[propName][0], prop.get('v').replace('<BR>', '\n')]
                else:
                    docProps[propName] = ['', prop.get('v').replace('<BR>', '\n')]
        #print(len(docProps))
        #print(docProps)
        masterPropList.append(docProps)
    return masterPropList
# THIS FUNCTION IS NOT BEING USED
def GetPropValuesMatching(docPropLabels, prechangeProps, postchangeProps):
    masterPropList = []
    for i, document in enumerate(prechangeProps):
        #print(document.get('documentId'))
        docProps = OrderedDict()
        docProps["DOCUMENTID"] = [str(document.get('documentId')), str(postchangeProps[i].get('documentId'))]
        #print (docProps)
        for docProp in docPropLabels:
            if docProp != "DOCUMENTID":
                docProps[docProp] = ['', '']
        for prop in document.get('properties'):
            propName = prop.get('k')
            if propName in docPropLabels:
                #print(propName)
                docProps[propName] = [prop.get('v').replace('<BR>', '\n'), '']
                #print(docProps[prop.get('k')])
            #print(docProps)              
        for prop in postchangeProps[i].get('properties'):
            #if not prop.get('k')[-4:] == "_COL" and prop.get('k') not in ('FILEDATE', 'FILENAME', 'FILE_PREFIX'):
            propName = prop.get('k')
            if propName in docPropLabels:
                if propName in docProps:
                    docProps[propName] = [docProps[propName][0], prop.get('v').replace('<BR>', '\n')]
                else:
                    docProps[propName] = ['', prop.get('v').replace('<BR>', '\n')]
        #print(len(docProps))
        #print(docProps)
        masterPropList.append(docProps)
    return masterPropList        
# THIS FUNCTION IS NOT BEING USED
def PopulateDocPropTable(service, labels, docProps):
    global sheetId
    UpdateSingleRange([labels], "B2")
    rows = []
    startRowNum = 3
    currentRowNum = 3
    startColIndex = 2
    ##
    endColIndex = startColIndex + len(docProps[0]) - 1 # subract 1 because we dont include docid or pre/post number
    
    print ("Setting column widths...")
    requests = SetAutoColumnWidth(startColIndex, endColIndex)
    SendUpdateRequests(service, requests)  
    
    prechangeRange = []
    postchangeRange = []

    dpLabelEqual = []
    dpLabelNotEqual = []

    borderRange =[{ "sheetId": sheetId,
                    "startColumnIndex": 0,
                    "endColumnIndex": endColIndex,
                    "startRowIndex": 1,
                    "endRowIndex": 2 }]


    compareNumber = 1    
    for document in docProps:
        # ADD DATA
        row1 = ["PRE.%06d" % compareNumber]        
        row2 = ["POS.%06d" % compareNumber]  
        for docProp in document:
            row1.append(document[docProp][0]) #prechange
            row2.append(document[docProp][1]) #postchange
        rows.append(row1)
        rows.append(row2)    
        #####
        prechangeRange.append({ "sheetId": sheetId,
                                "startColumnIndex": startColIndex,
                                "endColumnIndex": endColIndex,
                                "startRowIndex": currentRowNum-1,
                                "endRowIndex": currentRowNum})
        postchangeRange.append({ "sheetId": sheetId,
                                 "startColumnIndex": startColIndex,
                                 "endColumnIndex": endColIndex,
                                 "startRowIndex": currentRowNum,
                                 "endRowIndex": currentRowNum+1})
        dpLabelEqual.append("C%d=C%d" % (currentRowNum, currentRowNum+1))
        dpLabelNotEqual.append("C%d<>C%d" % (currentRowNum, currentRowNum+1))
        borderRange.append({ "sheetId": sheetId,
                             "startColumnIndex": 0,
                             "endColumnIndex": endColIndex,
                             "startRowIndex": currentRowNum,
                             "endRowIndex": currentRowNum+1})
        #requests = AddCompFormatRule(service, requests, endColIndex, currentRowNum-1)
        #AddCompFormatRule(service, "=D%s" % str(currentRowNum), endColIndex, currentRowNum)
        #####                  
        currentRowNum += 2
        compareNumber += 1
    # Add rows of data to sheet       
    UpdateSingleRange(rows, "A%s" % str(startRowNum))

    #for prerange in prechangeRange:
    #    for prop in prerange:
    #        print(prop)
    #pprint(prechangeRange)

    AddDPLabelCond(service, startColIndex, endColIndex, dpLabelEqual, dpLabelNotEqual)
    print ("Adding row borders...")
    AddRowBorders(service, borderRange)
    print ("Setting font to Calibri...")
    SetFont(service, endColIndex)  
    print ("Adding conditional formatting rules for each document...")
    requests = AddDPCompFormatRule(prechangeRange, postchangeRange)    
    return requests    
    
def AddDPCompFormatRule(prechangeRange, postchangeRange): 
    '''
    requests = [        
            {
              "addConditionalFormatRule": {
                "rule": {
                  "ranges": prechangeRange,
                  "booleanRule": {
                    "condition": {
                      "type": "NUMBER_EQ",
                      "values": [{ "userEnteredValue": "=C4"}]
                    },
                    "format": {
                      # Prechange, GREEN IF EQUAL  
                      "backgroundColor": {
                        "blue": 0.827451,  
                        "green": 0.91764706,
                        "red": 0.8509804,
                      }
                    }
                  }
                },
                "index": 0
              }            
            },
            {
              "addConditionalFormatRule": {
                "rule": {
                  "ranges": postchangeRange,
                  "booleanRule": {
                    "condition": {
                      "type": "NUMBER_EQ",
                      "values": [{"userEnteredValue": "=C3"}]
                    },
                    "format": {
                      # Prechange, GREEN IF EQUAL                          
                      "backgroundColor": {
                        "blue": 0.827451,  
                        "green": 0.91764706,
                        "red": 0.8509804,
                      }
                    }
                  }
                },
                "index": 0
              }            
            },
    '''
    requests = [
            {
              "addConditionalFormatRule": {
                "rule": {
                  "ranges": prechangeRange,
                  "booleanRule": {
                    "condition": {
                      "type": "NUMBER_NOT_EQ",
                      "values": [{"userEnteredValue": "=C4"}]
                    },
                    "format": {
                      # Prechange, RED IF NOT EQUAL
                      "backgroundColor": {
                        "blue": 0.8,  
                        "green": 0.8,
                        "red": 0.95686175,
                      }
                    }
                  }
                },
                "index": 0
              }            
            },            
            {
              "addConditionalFormatRule": {
                "rule": {
                  "ranges": postchangeRange,
                  "booleanRule": {
                    "condition": {
                      "type": "NUMBER_NOT_EQ",
                      "values": [{"userEnteredValue": "=C3"}]
                    },
                    "format": {
                      # Postchange, RED IF NOT EQUAL
                      "backgroundColor": {
                        "blue": 0.8,  
                        "green": 0.8,
                        "red": 0.95686175,
                      }
                    }
                  }
                },
                "index": 0
              }
            }]
    return requests

def AddDPLabelCond(service, startColIndex, endColIndex, dpLabelEqual, dpLabelNotEqual):
    rowIndex = 2
    docPropCondRange = [{"sheetId": sheetId,
                         "startColumnIndex": startColIndex,
                         "endColumnIndex": endColIndex,
                         "startRowIndex": rowIndex-1,
                         "endRowIndex": rowIndex}]
    equalRanges = ",".join(dpLabelEqual)                     
    equalValues = "=AND(%s)" % equalRanges                     
    notEqualRanges = ",".join(dpLabelNotEqual)                     
    notEqualValues = "=OR(%s)" % notEqualRanges                               
    requests = [
            {
              "addConditionalFormatRule": {
                "rule": {
                  "ranges": docPropCondRange,
                  "booleanRule": {
                    "condition": {
                      "type": "CUSTOM_FORMULA",
                      "values": [{ "userEnteredValue": equalValues}]
                    },
                    "format": {
                      # GREEN IF EQUAL  
                      "backgroundColor": {
                        "blue": 0.588,  
                        "green": 0.815,
                        "red": 0.568,
                      }
                    }
                  }
                },
                "index": 0
              }            
            },
            {
              "addConditionalFormatRule": {
                "rule": {
                  "ranges": docPropCondRange,
                  "booleanRule": {
                    "condition": {
                      "type": "CUSTOM_FORMULA",
                      "values": [{ "userEnteredValue": notEqualValues}]
                    },
                    "format": {
                      # RED IF NOT EQUAL  
                      "backgroundColor": {
                        "blue": 0.44,  
                        "green": 0.44,
                        "red": 0.874,
                      }
                    }
                  }
                },
                "index": 0
              }            
            }]    
    SendUpdateRequests(service, requests)        
   
def AddAlternatingColors():
    requests = [
                {'addBanding': {
                'bandedRange': {
                'range': {
                'sheetId': sheetId,
                'startRowIndex': 2,
                'startColumnIndex': 0,
                'endColumnIndex': 2,
                },
                'rowProperties': {
                'firstBandColor': {
                'red': 1,
                'green': .89,
                'blue': .74,
                },
                'secondBandColor': {
                'red': .776,
                'green': .905,
                'blue': 1,
                }
                },
                },
                },
                },
                {'updateSheetProperties': {
                    'properties': {'sheetId': sheetId, 'gridProperties': {'frozenRowCount': 2}},
                    'fields': 'gridProperties.frozenRowCount',
                }},
                {'updateSheetProperties': {
                    'properties': {'sheetId': sheetId, 'gridProperties': {'frozenColumnCount': 2}},
                    'fields': 'gridProperties.frozenColumnCount',
                }},               
                ]
    return requests

def SetFont(service, endColumnIndex):

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
          "startRowIndex": 1,
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
    }        
    ]

    SendUpdateRequests(service, requests)     

def AddRowBorders(service, borderRange):
    requests = []
    for ranges in borderRange:
        requests.append({'updateBorders': {'range': ranges, 'bottom': {"style": "SOLID",
                                                                     "width": 2,
                                                                     "color": {'red': 0,
                                                                               'green': 0,
                                                                               'blue': 0,}}}})
    SendUpdateRequests(service, requests)


def AddCompareSheet(rowCount):
    print("Start: Add new sheet to google doc")
    request = service.spreadsheets().get(spreadsheetId=spreadsheetId, fields="sheets.properties")
    response = request.execute()
    sheetNumbers = [0]
    for sheet in response.get('sheets'):
        sheetName = str(sheet.get('properties').get('title'))
        if "DP COMPARE" in sheetName:
            sheetNumbers.append(int(filter(str.isdigit, sheetName)))
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

def SetColumnWidth(startIndex, endIndex):
    requests = [{"updateDimensionProperties":{"range":{"sheetId": sheetId,                                                    
                                                       "dimension": "COLUMNS",
                                                       "startIndex": startIndex,
                                                       "endIndex": endIndex},
                                              "properties":{"pixelSize": 160},
                                              "fields": "pixelSize"}}]
    return requests 

def SetAutoColumnWidth(startIndex, endIndex):
    requests = [{"autoResizeDimensions":{"dimensions":{"sheetId": sheetId,                                                    
                                                      "dimension": "COLUMNS",
                                                      "startIndex": startIndex,
                                                      "endIndex": endIndex}}}]
    return requests                                                
        
def SendUpdateRequests(service, requests):
    global spreadsheetId
    body = {'requests': requests}
    response = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheetId,body=body).execute()
    return response


    
#####################################################
#####################################################
#####################################################
#####################################################
#####################################################
#####################################################        
global prechangeProps, postchangeProps, spreadsheetId, sheetId, sheetName, preId, postId, custId, service, startTime, elapsedTime 
spreadsheetId = "1-SWPPRg2i2IsTgUA-4BvpEkMyE1TBZUvmEHZw1zpWo4"
preId = 13811669
postId = 13811669
custId = 2001  

startTime = time.time()

# Authorize Google Sheets API credentials and build service
creds = GoogleAPIAuthorization()
service = discovery.build('sheets', 'v4', credentials=creds)

# Get list of doc prop names from prechange and postchange
#labels = GetDocProps()
labels = QueryMongo()
