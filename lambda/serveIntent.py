import json
import boto3
import logging
import time


def elicitSlots(sessionAttributes, intentName, slots, slotToElicit, message):
    response = {
        'sessionAttributes': sessionAttributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intentName,
            'slots': slots,
            'slotToElicit': slotToElicit,
            'message': {
                'contentType': 'PlainText',
                'content': message
            }
        }
    }
    return response

def close(sessionAttributes, fulfillmentState, message):
    return {
        'sessionAttributes': sessionAttributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillmentState,
            'message': {
                'contentType': 'PlainText',
                'content': message
            }
        }
    }

def athenaQueryHandler(queryString):
    client = boto3.client('athena')
    
    queryStart = client.start_query_execution(
        QueryString = queryString,
        QueryExecutionContext = {
            'Database': 'ipl-dataset'
        },
        ResultConfiguration = {
            'OutputLocation': 's3://marsbot-dataset-result/'
        }
    )

    queryId = queryStart['QueryExecutionId']
    status = 'RUNNING'
    while(status == 'RUNNING' or status == 'QUEUED'):
        response = client.get_query_execution(QueryExecutionId = queryId)
        status = response['QueryExecution']['Status']['State']
        if(status == 'RUNNING' or status == 'QUEUED'):
            time.sleep(0.200)

    results = client.get_query_results(QueryExecutionId = queryId)
    try:
        row = results['ResultSet']['Rows'][1].get('Data')
        return row
    except:
        return None

def validateDate(event):
    date = event['sessionAttributes'].get('date')
    queryString = 'SELECT * FROM "ipl-dataset"."ipl_dataset" WHERE "date"= \'{}\' limit 10;'.format(date)
    row = athenaQueryHandler(queryString)
    if not row:
        return False
    else:
        return True

def handleError(event):
    message = "I am sorry I couldn't get you there. Please try again!"
    return close(event['sessionAttributes'], 'Fulfilled', message)

def dispatchMom(event):
    date = event['sessionAttributes'].get('date')
    queryString = 'SELECT "player_of_match" FROM "ipl-dataset"."ipl_dataset" WHERE "date"= \'{}\';'.format(date)
    row = athenaQueryHandler(queryString)
    mom = row[0]['VarCharValue']
    message = "{} had won the Man of the match award".format(mom)
    
    return close(event['sessionAttributes'], 'Fulfilled', message)

def dispatchVenue(event):
    date = event['sessionAttributes'].get('date')
    queryString = 'SELECT "venue" FROM "ipl-dataset"."ipl_dataset" WHERE "date"= \'{}\';'.format(date)
    row = athenaQueryHandler(queryString)
    venue = row[0]['VarCharValue']
    message = "The match was played at {}".format(venue)
    
    return close(event['sessionAttributes'], 'Fulfilled', message)

def dispatchToss(event):
    date = event['sessionAttributes'].get('date')
    queryString = 'SELECT "toss_winner", "toss_decision" FROM "ipl-dataset"."ipl_dataset" WHERE "date"= \'{}\';'.format(date)
    row = athenaQueryHandler(queryString)
    tossWinner = row[0]['VarCharValue']
    tossDecision = row[1]['VarCharValue']
    message = "{} had won the toss and elected to {} first".format(tossWinner, tossDecision)
    
    return close(event['sessionAttributes'], 'Fulfilled', message)

def dispatchUmpires(event):
    date = event['sessionAttributes'].get('date')
    queryString = 'SELECT "umpire1", "umpire2" FROM "ipl-dataset"."ipl_dataset" WHERE "date"= \'{}\';'.format(date)
    row = athenaQueryHandler(queryString)
    umpire1 = row[0]['VarCharValue']
    umpire2 = row[1]['VarCharValue']
    message = "The onfield umpires were {} and {}".format(umpire1, umpire2)
    
    return close(event['sessionAttributes'], 'Fulfilled', message)

def dispatchMargin(event):
    date = event['sessionAttributes'].get('date')
    queryString = 'SELECT "winner", "result", "win_by_runs", "win_by_wickets" FROM "ipl-dataset"."ipl_dataset" WHERE "date"= \'{}\';'.format(date)
    row = athenaQueryHandler(queryString)
    winner = row[0]['VarCharValue']
    result = row[1]['VarCharValue']
    winByRuns = row[2]['VarCharValue']
    winByWickets = row[3]['VarCharValue']
    message = ""
    if(result == 'normal'):
        message = "{} had won by ".format(winner)
        if(winByWickets == '0'):
            message = message + "{} runs".format(winByRuns)
        else:
            message = message + "{} wickets".format(winByWickets)
    elif(result == "tie"):
        message = "The match was tied. {} won in super over.".format(winner)
    else:
        message = "The match was washed out. No result."        

    return close(event['sessionAttributes'], 'Fulfilled', message)


def dispatchWinner(event):
    date = event['sessionAttributes'].get('date')
    queryString = 'SELECT "winner", "result" FROM "ipl-dataset"."ipl_dataset" WHERE "date"= \'{}\';'.format(date)
    row = athenaQueryHandler(queryString)
    winner = row[0]['VarCharValue']
    result = row[1]['VarCharValue']
    if(result == 'normal'):
        message = "{} had won that match".format(winner)
    elif(result == "tie"):
        message = "The match was tied. {} won in super over.".format(winner)
    else:
        message = "The match was washed out. No result." 
    
    return close(event['sessionAttributes'], 'Fulfilled', message)
    

def dispatchTeams(event):
    date = event['sessionAttributes'].get('date')
    queryString = 'SELECT "team1", "team2" FROM "ipl-dataset"."ipl_dataset" WHERE "date"= \'{}\';'.format(date)
    row = athenaQueryHandler(queryString)
    team1 = row[0]['VarCharValue']
    team2 = row[1]['VarCharValue']
    message = "That match was played between {} and {}".format(team1, team2)
    
    return close(event['sessionAttributes'], 'Fulfilled', message)



def intentHandler(event):
    date = event['currentIntent']['slots'].get('date', None)
    if not date:
        date = event['sessionAttributes'].get('date', None)
        if not date:
            message = "Please specify a date for the match"
            return elicitSlots(event['sessionAttributes'], event['currentIntent']['name'], event['currentIntent']['slots'], 'date', message)
    
    sessionAttributes = {
        "date": date,
        "lastIntent": event['currentIntent']['name']
    }
    event['sessionAttributes'] = sessionAttributes

    if not validateDate(event):
        message = "There was no match on {}".format(date)
        return close(event['sessionAttributes'], 'Fulfilled', message)
    
    
    intent = event['currentIntent']['name']

    if(intent == 'getTeams'):
        return dispatchTeams(event)
    
    elif(intent == 'winner'):
        return dispatchWinner(event)
    elif(intent == 'tossWinner'):
        return dispatchToss(event)
    elif(intent == 'venue'):
        return dispatchVenue(event)
    elif(intent == 'umpireab'):
        return dispatchUmpires(event)
    elif(intent == 'POMatch'):
        return dispatchMom(event)
    elif(intent == 'winningMargin'):
        return dispatchMargin(event)
    else:
        return handleError(event)

def switchIntent(event):
    date = event['currentIntent']['slots'].get('date', None)
    if not date:
        message = "Which date?"
        return elicitSlots(event['sessionAttributes'], event['currentIntent']['name'], event['currentIntent']['slots'], 'date', message)
    else:
        nextEvent = event['sessionAttributes'].get('lastIntent', None)
        event['currentIntent']['name'] = nextEvent
        return intentHandler(event)

def lambda_handler(event, content):
    intentName = event['currentIntent'].get('name', None)
    if(intentName == 'switchIntent'):
        return switchIntent(event)
    return intentHandler(event)