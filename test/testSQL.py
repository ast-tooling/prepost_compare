import mysql.connector
import sys
from base64 import b64decode

mydb = mysql.connector.connect(
  host="imdb",
  user="cweakley",
  passwd="aMzjWrrYRT9jSBh",
  database="imstage01"
)

#mycursor = mydb.cursor()

#mycursor.execute("SELECT documentId, FFDId from fsidocument WHERE customerId = 2192")
#mycursor.execute("SELECT documentId FROM fsidocument WHERE customerid = 2700 AND batchid = 13816259 \
#				  AND FFDId IN (SELECT FFDId FROM fsiFFD WHERE customerId = 2700 AND itemType = 'O')")

#myresult = mycursor.fetchall()
#myresult = list(mycursor.fetchall())

#for x in myresult:
#  print(x)
#import sys
#print(len(myresult))

sqlConnFile = r"C:\Users\cweakley\AppData\Roaming\SQLyog\sqlyog.ini"
inFile = open(sqlConnFile, 'rb')
foundConnection = False
connections = {}
for line in inFile.readlines():
	#print(line)
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

print(userName, password)					 		
		

def decode_password(encoded):
    tmp = bytearray(b64decode(encoded))

    for i in range(len(tmp)):
        tmp[i] = rotate_left(tmp[i], 8)

    return tmp.decode('utf-8')

# https://gist.github.com/cincodenada/6557582
def rotate_left(num, bits):
    bit = num & (1 << (bits-1))
    num <<= 1
    if(bit):
        num |= 1
    num &= (2**bits-1)

    return num

print(decode_password(password))