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

def Log ( logLevel, logMsg  ):
    print("%s%s") % (logLevel, logMsg)

# munges up values from params and config.json to produce a meta config object that can be passed around and referenced consistently
def ConfigFactory( event ):
    Log ( 'INFO: ', 'building config'  )
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
        healthCheckTimeOut = configJson['globalConfig']['healthCheckTimeOut']
        stacks = configJson['stacks']
        action = event['action']

        if action == 'healthcheck':
            stack, uid, stackId = None, None, None
        else:
            stack = event['stack']
            uid = event['uid']
            stackId = "%s-%s" % (stack, uid)

        if action == 'update':
            desiredStateId = event['desiredState']
            desiredState = configJson['stacks'][stackId][desiredStateId]
        else:
            desiredStateId, desiredState = None, None

        configObj = collections.namedtuple('Config', [
            'debug',
            'region',
            'stateTableName',
            'stateTableInitialReadCapacityUnits',
            'stateTableInitialWriteCapacityUnits',
            'healthCheckTimeOut',
            'stacks',
            'stackId',
            'action',
            'desiredStateId',
            'desiredState'
        ])

        config = configObj(
            debug,
            region,
            stateTableName,
            stateTableInitialReadCapacityUnits,
            stateTableInitialWriteCapacityUnits,
            healthCheckTimeOut,
            stacks,
            stackId,
            action,
            desiredStateId,
            desiredState
        )

        Debug ( None, config._asdict() )
        Log ( 'INFO: ', 'building config DONE'  )
        return config

def TestStateTable ( client, conf ):
    Log ( 'INFO: ', 'testing state table'  )
    Debug ( sys._getframe().f_code.co_name, locals() )

    stateTableExists = True
    try:
        resp = client.describe_table(TableName=conf.stateTableName)
    except:
        stateTableExists = False
        Log ( '    WARNING: ', 'stateTable does not exist'  )

    Debug ( None, {'stateTableExists':stateTableExists} )
    Log ( 'INFO: ', 'testing state table DONE'  )
    return stateTableExists

def CreateStateTable ( client, conf ):
    Log ( 'INFO: ', 'creating state table'  )
    Debug ( sys._getframe().f_code.co_name, locals() )

    table = client.create_table(
        TableName=conf.stateTableName,
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
            'ReadCapacityUnits': conf.stateTableInitialReadCapacityUnits,
            'WriteCapacityUnits': conf.stateTableInitialWriteCapacityUnits
        }
    )
    Log ( '    INFO: ', ('initiated creation of table: (%s)' % table['TableDescription']['TableArn'])  )
    # do not continue until TableStatus == 'ACTIVE'
    while True:
        try:
            resp = client.describe_table(TableName=conf.stateTableName)
            time.sleep(1)
        except:
            pass
        if resp:
            Log ( '    INFO: ', ('table status: (%s)' % resp['Table']['TableStatus'])  )
        if resp['Table']['TableStatus'] == 'ACTIVE':
            break

    Debug ( None, {"resp": resp} )
    Log ( 'INFO: ', 'creating state table DONE'  )

def InitializeStateTable ( dynamodb, conf ):
    Log ( 'INFO: ', 'initializing state table'  )
    Debug ( sys._getframe().f_code.co_name, locals() )

    table = dynamodb.Table(conf.stateTableName)
    for stack in conf.stacks:
        resp = table.get_item(
            Key={
                'stackId': stack
            }
        )
        if 'Item' in resp:
            Debug ( None, {stack: "exists"} )
        else:
            Log ( '    WARN: ', ('no record exists for (%s), creating it' % stack)  )
            item = {'stackId': stack,'currentState': 'idle', 'lastHeartBeatTime': 0}
            resp = table.put_item(Item=item)
            Log ('        INFO: ', ('table.put_item response: (%s)' % resp))

    stateTable = dynamodb.Table(conf.stateTableName)
    Debug ( None, {"stateTable": stateTable} )
    Log ( 'INFO: ', 'initializing state table DONE'  )
    return stateTable


def HeartBeat ( stateTable, conf ):
    Log ( 'INFO: ', ('heartbeat for (%s)' % conf.stackId))
    Debug ( sys._getframe().f_code.co_name, locals() )

    now = int(time.time())
    resp = stateTable.update_item(
        Key={
            'stackId': conf.stackId
        },
        UpdateExpression="set lastHeartBeatTime = :lh",
        ExpressionAttributeValues={
            ':lh': now
        },
        ReturnValues="UPDATED_NEW"
    )

    Debug ( None, {"heartBeatResp": resp} )
    Log ( 'INFO: ', ('heartbeat for (%s) DONE' % conf.stackId))

def HealthCheck ( stateTable, conf ):
    Log ( 'INFO: ', 'healthcheck')
    Debug ( sys._getframe().f_code.co_name, locals() )

    for stack in conf.stacks:
        resp = stateTable.get_item(
            Key={
                'stackId': stack
            }
        )
        Debug ( None, {"HealthCheckResp": resp} )
        lastHeartBeatTime = resp['Item']['lastHeartBeatTime']
        now = int(time.time())
        timeSinceLastHeartBeat = now - lastHeartBeatTime
        if timeSinceLastHeartBeat > conf.healthCheckTimeOut:
            Log ( '    WARN: ', ('healthcheck for  (%s) exeeded timeout' % stack))
            # change conf.stackId and conf.desiredState from Null to the values needed, and pass to ChangeState
        Debug ( None, {"lastHeartBeatTime": lastHeartBeatTime,"now": now,"timeSinceLastHeartBeat": timeSinceLastHeartBeat} )

    Log ( 'INFO: ', 'healthcheck DONE')


def ChangeState ( stateTable, conf ):
    Log ( 'INFO: ', ('state change requested for (%s)' % conf.stackId))
    Debug ( sys._getframe().f_code.co_name, locals() )
    # TestState

def lambda_handler ( event, context ):
    conf = ConfigFactory ( event )
    client = boto3.client('dynamodb', region_name=conf.region) # ddb client object, used for high level operations: describe/create tables, etc.
    dynamodb = boto3.resource('dynamodb', region_name=conf.region) # ddb table object, used for table/record level operations: read/write/update/delete, etc.

    if not TestStateTable ( client, conf ):
        CreateStateTable ( client, conf )

    stateTable = InitializeStateTable ( dynamodb, conf )

    if conf.action == 'heartbeat':
        HeartBeat ( stateTable, conf )

    elif conf.action == 'healthcheck':
        HealthCheck ( stateTable, conf )

    elif conf.action == 'changeState':
        ChangeState ( stateTable, conf )

    else:
        raise ValueError('This should never happen. Ensure \'event\' parameter = \'heartbeat\' | \'healthcheck\' | \'update\'' )

'''
#todo
scheduled lambda call for healtcheck

test to ensure each stack has an idle scenario, alert when not true
function to ensure all tables have a workflow idle rule, with exclude list, SNS alert when this is not the case
throughput currently adjusted table by table, including indexes. Add functionality to break out indexes explicitly by name, and also by regex
'''
