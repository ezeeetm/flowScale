#!/usr/bin/env python
import json
import boto3
import time
import collections
import sys


def Debug ( headerMsg, debugMsgDict ):
    if debug:
        if headerMsg:
            print("############## DEBUG: %s") % ( headerMsg )
        for i in debugMsgDict:
            print "%s: %s" % (i, debugMsgDict[i])

# munges up values from params and config.json to produce a meta config object can be passed around idempotently
def ConfigFactory( event ):
    with open('config.json', 'r') as configFile:
        configStr = configFile.read()
        configJson = json.loads(configStr)

        global debug # yo dawg...
        debug = configJson['globalConfig']['debug']
        Debug ( sys._getframe().f_code.co_name, {'params':event} )
        region = configJson['globalConfig']['region']
        stateTableInitialReadCapacityUnits = configJson['globalConfig']['stateTableInitialReadCapacityUnits']
        stateTableInitialWriteCapacityUnits = configJson['globalConfig']['stateTableInitialWriteCapacityUnits']
        stateTableName = configJson['globalConfig']['stateTable']
        action = event['action']

        if action == 'healthcheck':
            stack, uid, stackId = None, None, None
        else:
            stack = event['stack']
            uid = event['uid']
            stackId = "%s-%s" % (stack, uid)

        if action == 'update':
            stateId = event['stateId']
            desiredState = configJson['stacks'][stackId][stateId]
        else:
            stateId, desiredState = None, None

        configObj = collections.namedtuple('Config', [
            'debug',
            'region',
            'stateTableName',
            'stateTableInitialReadCapacityUnits',
            'stateTableInitialWriteCapacityUnits',
            'stackId',
            'action',
            'stateId',
            'desiredState'
        ])

        config = configObj(
            debug,
            region,
            stateTableName,
            stateTableInitialReadCapacityUnits,
            stateTableInitialWriteCapacityUnits,
            stackId,
            action,
            stateId,
            desiredState
        )

        Debug ( None, config._asdict() )
        return config


#def InitStateTable ( client, dynamodb, conf ):

'''
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
'''


def lambda_handler ( event, context ):
    conf = ConfigFactory ( event )
    client = boto3.client('dynamodb', region_name=conf.region) # ddb client object, used for high level operations: describe/create tables, etc.
    dynamodb = boto3.resource('dynamodb', region_name=conf.region) # ddb table object, used for table read/write/update/delete operations
    #InitStateTable ( client, dynamodb, conf ) #stateTable =
