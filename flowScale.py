#!/usr/bin/env python
import json
import boto3

def GetConfig():
    with open('config.json', 'r') as configFile:
        configStr = configFile.read()
        configJson = json.loads(configStr)
        return configJson

def InitStateTable ( client, stateTableName, stateTableInitialReadCapacityUnits, stateTableInitialWriteCapacityUnits ):
    try:
        resp = client.describe_table(TableName=stateTableName)
        print("stateTable %s status: %s") % ( stateTableName, resp['Table']['TableStatus'])
    except:
        print("%s doesn't exist, hence creating it ") % ( stateTableName )
        table = client.create_table(
            TableName=stateTableName,
            KeySchema=[
                {
                    'AttributeName': 'stackId',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'stackId',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': stateTableInitialReadCapacityUnits,
                'WriteCapacityUnits': stateTableInitialWriteCapacityUnits
            }
        )
        print('created table: ', table['TableDescription']['TableArn'])
    return

def HeartBeat ( stackId ):
    print("HeartBeat: %s") % ( stackId )
    return

def HealthCheck ( stackId ):
    print("HealthCheck: %s") % ( stackId )
    return


def lambda_handler ( event, context ):
    # infer config values from params and config.json
    config = GetConfig()
    debug = config['globalConfig']['debug']
    region = config['globalConfig']['region']
    stateTableName = config['globalConfig']['stateTable']
    stateTableInitialReadCapacityUnits = config['globalConfig']['stateTableInitialReadCapacityUnits']
    stateTableInitialWriteCapacityUnits = config['globalConfig']['stateTableInitialWriteCapacityUnits']
    stack = event['stack']
    uid = event['uid']
    eventType = event['type']
    stackId = "%s-%s" % (stack, uid)

    # dynamo connection object, do this at app start only, not for each operation. Creates admin table to manage state, if it does not exist
    client = boto3.client('dynamodb', region_name=region)
    InitStateTable ( client, stateTableName, stateTableInitialReadCapacityUnits, stateTableInitialWriteCapacityUnits )

    if eventType == 'heartbeat':
        HeartBeat ( stackId )
        return
    elif eventType == 'healthcheck':
        HealthCheck ( stackId )
        return
    elif eventType == 'update':
        scenarioId = event['scenario']
        scenario = config['stackConfigs'][stackId][scenarioId]
    else:
        print('This should never happen, how did you get here?')

    if debug == True:
        print("stackId: %s") % ( stackId )
        print("scenarioId: %s") % ( scenarioId )
        print("scenario: %s") % ( scenario )
        for changeGroup in scenario:
            print("changeGroup: %s") % ( changeGroup )

'''
#todo
if action = heartbeat update last hearbeat time
heartbeat threshold to config
scheduled call with type heartbeat check if over configured threshold, change scenario to 'idle'

ensure request to change scenarios is ignored when already in that scenario

cw log function
test to ensure each stack has an idle scenario
function to ensure all tables have a workflow idle rule, with exclude list, SNS alert when this is not the case
'''
