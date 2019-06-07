from __future__ import print_function
from pprint import pprint
import pickle
import os.path
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
    global prechangeProps, postchangeProps, preId, postId, custId 
    fsidocprops = InitMongoClient()
    # START: QUERY FOR DP #
    #######################
    #preId = int(app.getEntry('ePrechangeId'))
    #postId = int(app.getEntry('ePostchangeId'))
    #custId = int(app.getEntry('eCustId')) 
    prechangeProps = list(fsidocprops.find({'batchId': preId, 'customerId': custId}))
    postchangeProps = list(fsidocprops.find({'batchId': postId, 'customerId': custId}))#,{'_id':0, 'properties':1}))
    #print(prechangeProps)
    # END: QUERY FOR DP #
    #####################

    # START: ERROR HANDLING (NOT REALLY) #
    ######################################
    errorMsg = ''
    if len(prechangeProps) == 0:
        errorMsg += "Prechange batch: %s not found..." % preId
    if len(postchangeProps) == 0:
        errorMsg += "\nPostchange batch: %s not found..." % postId
    if len(errorMsg) > 0:
        print(errorMsg.strip())

    # END: ERROR HANDLING (NOT REALLY) #
    ####################################

    # For now, we are only worrying about batches that have the same number of records
    if len(prechangeProps) == len(postchangeProps):      
        docPropLabels = GetLabels(prechangeProps, postchangeProps)
        docPropLabels.sort()
        #masterPropList = GetPropValues(docPropLabels, prechangeProps, postchangeProps)
        docPropLabels.insert(0, "DOCUMENTID")
        return docPropLabels
    else:
        print("Prechange document count does not match Postchange document count")
        exit

def GetLabels(prechangeProps, postchangeProps):
    docPropLabels = []
    #for document in prechangeProps:
    #    print(document)
    #app.startScrollPane
    for document in prechangeProps:
        for prop in document.get('properties'):
            #print(prop.get('k'))
            if not prop.get('k')[-4:] == "_COL" and prop.get('k') not in ('FILEDATE', 'FILENAME', 'FILE_PREFIX', 'XML_DATA', 'BT_PRINT_FILE_NAME'):
                if prop.get('k') not in docPropLabels:
                    docPropLabels.append(str(prop.get('k')))
    #for document in prechangeProps:
    #    print(document)                      
    for document in postchangeProps:
        for prop in document.get('properties'):
            if not prop.get('k')[-4:] == "_COL" and prop.get('k') not in ('FILEDATE', 'FILENAME', 'FILE_PREFIX', 'XML_DATA', 'BT_PRINT_FILE_NAME'):
                if prop.get('k') not in docPropLabels:
                    docPropLabels.append(str(prop.get('k')))                                                        
    return docPropLabels 

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
global prechangeProps, postchangeProps, spreadsheetId, sheetId, sheetName, preId, postId, custId 
spreadsheetId = "1pr488qjVfuFkjFglI8VBMatl0-FRGjgws0pWwNIpw4Y"
preId = 9388360
postId = 9396283
custId = 2591  

# Authorize Google Sheets API credentials and build service
creds = GoogleAPIAuthorization()
service = discovery.build('sheets', 'v4', credentials=creds)



# Get list of doc prop names from prechange and postchange
labels = GetDocProps()
# Get list of all doc prop values from prechange and postchange
docProps = GetPropValues(labels, prechangeProps, postchangeProps)
# Add new Compare Sheet and get sheetId/sheetName
rowCount = (len(docProps) * 2) + 2
addSheetResponse = SendUpdateRequests(service, AddCompareSheet(rowCount))
sheetId = addSheetResponse.get('replies')[0].get('addSheet').get('properties').get('sheetId')
sheetName = str(addSheetResponse.get('replies')[0].get('addSheet').get('properties').get('title'))
# Add properties and their values to sheet, returns list of formatting requests 
formattingRequests = PopulateDocPropTable(service, labels, docProps)
# Send conditional formatting requests
SendUpdateRequests(service, formattingRequests)
# Send alternating colors request
SendUpdateRequests(service, AddAlternatingColors())

#request = service.spreadsheets().get(spreadsheetId=spreadsheetId, fields="sheets.properties")
#response = request.execute()
pprint(addSheetResponse)
print(sheetId)
