import time
import jwt
import requests
import json
import socket
import os
from pymongo import MongoClient
import datetime

def get_database():
    # Connection to my MongoAtlas DB, Also change to local database later.
    #local uri
    uri = "mongodb+srv://[username]:[password]@[mongodb cluster address]"
    # Create a connection using to DB
    client = MongoClient(uri)
    # Creating/specifying the database

    return client['Spaces_Data']


def get_API_Key_and_auth():
    # Gets public key from spaces and places in correct format
    print("-- No API Key Found --")
    pubKey = requests.get('https://partners.dnaspaces.io/client/v1/partner/partnerPublicKey/')  # Change this to .eu if needed
    pubKey = json.loads(pubKey.text)
    pubKey = pubKey['data'][0]['publicKey']
    pubKey = '-----BEGIN PUBLIC KEY-----\n' + pubKey + '\n-----END PUBLIC KEY-----'

    # Gets user to paste in generated token from app
    token = input('Enter token here: ')

    # Decodes JSON Web Token to get JSON out
    decodedjwt = jwt.decode(token, pubKey, algorithms=["RS256"], options={"verify_signature": False})
    decodedjwt = json.dumps(decodedjwt, indent=2)

    # picks up required values out of jwt
    decodedjwtJSON = json.loads(decodedjwt)
    appId = decodedjwtJSON['appId']
    activationRefId = decodedjwtJSON['activationRefId']

    # creates payloads and headers ready to activate app
    authKey = 'Bearer ' + token
    payload = {'appId': appId, 'activationRefId': activationRefId}
    header = {'Content-Type': 'application/json', 'Authorization': authKey}

    # Sends request to spaces with all info about jwt to confirm its correct, if it is, the app will show as activated
    activation = requests.post(
        'https://partners.dnaspaces.io/client/v1/partner/activateOnPremiseApp/', headers=header, json=payload)  # Change this to .eu if needed

    # pulls out activation key
    activation = json.loads(activation.text)
    apiKey = activation['data']['apiKey']

    # Writes activation key to file. This key can be used to open up Firehose connection
    f = open("API_KEY.txt", "a")
    f.write(apiKey)
    f.close()
    return apiKey


# Creates usable object to write to DB
dbname = get_database()

# work around to get IP address on hosts with non resolvable hostnames
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(("8.8.8.8", 80))
IP_ADRRESS = s.getsockname()[0]
s.close()
url = 'http://' + str(IP_ADRRESS) + '/update/'

# Tests to see if we already have an API Key
try:
    if os.stat("API_KEY.txt").st_size > 0:
        # If we do, lets use it
        f = open("API_KEY.txt")
        apiKey = f.read()
        f.close()
    else:
        # If not, lets get user to create one
        apiKey = get_API_Key_and_auth()
except:
    apiKey = get_API_Key_and_auth()

# overwrite previous log file
#f = open("log_file.json", 'w')

# Opens a new HTTP session that we can use to terminate firehose onto
s = requests.Session()
s.headers = {'X-API-Key': apiKey}
r = s.get(
    'https://partners.dnaspaces.io/api/partners/v1/firehose/events', stream=True)  # Change this to .eu if needed

# Jumps through every new event we have through firehose
print("Starting Stream")
#logging event
now = datetime.datetime.now()
filename_pattern = now.strftime('%y%m%d'+'.log')

for line in r.iter_lines():
    if line:
        try:
            # f.write(str(json.dumps(json.loads(line), indent=4, sort_keys=True)))

            # decodes payload into useable format
            decoded_line = line.decode('utf-8')
            #decoded_line = decoded_line.replace("false", "\"false\"")
            #decoded_line = decoded_line.replace(" ", "")
            event = json.loads(decoded_line)
            eventType = event['eventType']

            # Creates/Specifies the collection. Collections are grouped by their event type
            collection_name = dbname[eventType]
            collection_name.insert_one(event)
            #time.sleep(1) #sleep for 1 second, prevent too much load
        except Exception as e:
            # print exceptions
            # throws decode error and continue
            print(e)

    # print(event)
    print(now.strftime('%X'+'_'+'%f' + ': ') + eventType)