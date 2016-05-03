#!/usr/bin/env python
import json
import boto3
import time

def GetConfig():
    with open('config.json', 'r') as configFile:
        configStr = configFile.read()
        configJson = json.loads(configStr)
        return configJson

def InitStateTable ( client, dynamodb, stateTableName, stateTableInitialReadCapacityUnits, stateTableInitialWriteCapacityUnits, config ):
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
        print("created table: %s") % ( table['TableDescription']['TableArn'] )

        # don't write to new table until it's ready
        while True:
            try:
                resp = client.describe_table(TableName=stateTableName)
                time.sleep(1)
            except:
                pass
            if resp['Table']['TableStatus'] == 'ACTIVE':
                break

        # populate table with initial values
        print("intializing stacks data:")
        stacks = config['stackConfigs']
        table = dynamodb.Table(stateTableName)
        for stack in stacks:
                item = {'stackId': stack,'currentState': 'idle', 'lastHeartBeatTime': 0}
                table.put_item(Item=item)
                print("    %s") % ( item )

    stateTable = dynamodb.Table(stateTableName)
    return stateTable

def HeartBeat ( stateTable, stackId ):
    now = int(time.time())
    print("HeartBeat for stack %s @ %s epoch time") % ( stackId, now )
    response = stateTable.update_item(
        Key={
            'stackId': stackId
        },
        UpdateExpression="set lastHeartBeatTime = :lh",
        ExpressionAttributeValues={
            ':lh': now
        },
        ReturnValues="UPDATED_NEW"
    )
    print("UpdateItem succeeded: %s") % ( response )
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
    dynamodb = boto3.resource('dynamodb', region_name=region)
    stateTable = InitStateTable ( client, dynamodb, stateTableName, stateTableInitialReadCapacityUnits, stateTableInitialWriteCapacityUnits, config )

    if eventType == 'heartbeat':
        HeartBeat ( stateTable, stackId )
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
add initial heartbeat to InitStateTable
scheduled call with type heartbeat check if over configured threshold, change scenario to 'idle'

ensure request to change scenarios is ignored when already in that scenario

cw log function
test to ensure each stack has an idle scenario
function to ensure all tables have a workflow idle rule, with exclude list, SNS alert when this is not the case
'''
