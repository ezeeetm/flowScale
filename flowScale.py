#!/usr/bin/env python
import json

def GetConfig():
    with open('config.json', 'r') as configFile:
        configStr = configFile.read()
        configJson = json.loads(configStr)
        return configJson



#def lambda_handler ( event, context ):
if __name__ == "__main__":
    config = GetConfig()
    debug = config['globalConfig']['debug']
    stack = event['stack']
    uid = event['uid']
    stackId = "%s-%s" % (stack, uid)
    scenarioId = event['scenario']
    scenario = config['stackConfigs'][stackId][scenarioId]

    if debug == True:
        print("stackId: %s") % ( stackId )
        print("scenarioId: %s") % ( scenarioId )
        print("scenario: %s") % ( scenario )



'''
#todo
cw log function
function to ensure all tables have a workflow idle rule, with exclude list, SNS alert when this is not the case
ensure request to change scenarios is ignored when already in that scenario
'''
