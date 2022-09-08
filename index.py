import time
import jwt
import requests
import json
import socket
import os
import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

#load credentials
load_dotenv()
mongo_username = os.getenv("MONGO_USERNAME")
mongo_password = os.getenv("MONGO_PASSWORD")
token = os.getenv("API_KEY")

#open up a log file
#filename pattern
filename_pattern = 'logging_' + datetime.datetime.now().strftime('%y%m%d'+'.log')

#logfile = open(filename_pattern,'a')

def get_database():
    # Connection to my MongoAtlas DB, Also change to local database later.
    uri = "mongodb+srv://"+mongo_username+":"+mongo_password+"@cluster0.hozznpa.mongodb.net/?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE"
    # Create a connection using to DB
    client = MongoClient(uri)
    # Creating/specifying the database

    return client['Spaces_Data']


def get_API_Key_and_auth():
    # Gets public key from spaces and places in correct format
    print("-- No API Key Found --")
    pubKey = requests.get('https://partners.dnaspaces.io/client/v1/partner/partnerPublicKey/')  # Change this to .io if needed
    pubKey = json.loads(pubKey.text)
    pubKey = pubKey['data'][0]['publicKey']
    pubKey = '-----BEGIN PUBLIC KEY-----\n' + pubKey + '\n-----END PUBLIC KEY-----'

    #get token from .env
    print('Reading API key...')
    
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
        'https://partners.dnaspaces.io/client/v1/partner/activateOnPremiseApp/', headers=header, json=payload)  # Change this to .io if needed

    # pulls out activation key
    activation = json.loads(activation.text)
    apiKey = activation['data']['apiKey']

    return apiKey


# Creates usable object to write to DB
dbname = get_database()

# work around to get IP address on hosts with non resolvable hostnames
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(("8.8.8.8", 80))
IP_ADRRESS = s.getsockname()[0]
s.close()
url = 'http://' + str(IP_ADRRESS) + '/update/'

# Opens a new HTTP session that we can use to terminate firehose onto
s = requests.Session()
s.headers = {'X-API-Key': token}
r = s.get(
    'https://partners.dnaspaces.io/api/partners/v1/firehose/events', stream=True)  # Change this to .io if needed

# Jumps through every new event we have through firehose
print("Starting Stream")

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
            #time.sleep(1) # slow down stream
            collection_name.insert_one(event)
            #logging to log folder
            #for monitor or debugging propose, uncomment below
            #logfile.write(str(json.dumps(json.loads(line), indent=4, sort_keys=True)) + '\n')
        except Exception as e:
            # print ERROR
            # throws decode error and continue
            print(e)
            #also logging errors
            #logfile.write(str(e) + '\n')

    # print(event)
    print(datetime.datetime.now().strftime('%X'+ ': ') + eventType)

#logfile.close()