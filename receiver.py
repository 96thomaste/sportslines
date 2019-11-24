import requests
import json
import re
import time
import pymongo
import datetime
import timestring

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]
eventCol = mydb["events"]
lineCol = mydb["lines"]
percentCol = mydb["percents"]
betCol = mydb["bets"]
starting_url = "https://free.sportsinsights.com/free-odds/free-odds-frame.aspx?SportGroup=sg1"
urgent_url = "https://free.sportsinsights.com/dataservice/jsonhandler.aspx/GetUrgent"
message_url = "https://free.sportsinsights.com/dataservice/jsonhandler.aspx/GetMessages"
latest_alert_id = 0
leagues = {}
rleagues = {}


def request(urgent=False):
    global latest_alert_id
    data = {"data": "\"\"" if urgent else "" + str(latest_alert_id) + ""}
    headers = {'content-type': 'application/json', 'accept': 'application/json'}
    r = requests.post(urgent_url if urgent else message_url, data=json.dumps(data), headers=headers)
    resp = json.loads(r.json()['d'])
    return resp


class SportEvent:

    def __init__(self, x):
        val = x['Val']
        val = list(filter(lambda x: True, val.split('_')))
        self._id = int(x['Key'])
        self.data = dict()
        self.data["league"] = rleagues[int(val[7])].strip().strip("-")
        self.data["team_home"] = val[1].strip().strip("-")
        self.data["team_visit"] = val[2].strip().strip("-")
        self.data["team_home_short"] = val[15].strip().strip("-")
        self.data["team_visit_short"] = val[16].strip().strip("-")
        self.data["period_short"] = val[9].strip().strip("-")
        self.data["period_time"] = val[10].strip().strip("-")
        self.data["visitor_score"] = val[11].strip().strip("-")
        self.data["home_score"] = val[12].strip().strip("-")
        self.data["start_time"] = timestring.Date(x['EventDateTimeUser']).date


def process_event(event):
    eventCol.update({'_id': event._id}, event.data, upsert=True)


def process_messages(messages):
    for message in messages:
        key = {'_id': str(message['event_id']) + "_" + str(message['sportsbook_id']) }
        data = {}
        # Line
        if message['alert_type_id'] == 1:
            data = message["details"]
            data["date"] = timestring.Date(message["created_date"]).date
            lineCol.update(key, data, upsert=True)
        # Percent
        if message['alert_type_id'] == 2:
            data = message["details"]
            data["date"] = timestring.Date(message["created_date"]).date
            percentCol.update(key, data, upsert=True)
        # Bets
        if message['alert_type_id'] == 3:
            key = {'_id': str(message['event_id'])}
            data = message["details"]
            data["date"] = timestring.Date(message["created_date"]).date
            betCol.update(key, data, upsert=True)
        # Game
        if message['alert_type_id'] == 4:
            key = {'_id': message['event_id']}
            data['home_score'] = message['details']['home_score']
            data['visitor_score'] = message['details']['visitor_score']
            data['period_short'] = message['details']['period_short']
            data['period_time'] = message['details']['period_time']
            data['home_score'] = message['details']['home_score']
            eventCol.update(key, data, upsert=True)


def get_starting_data():
    global latest_alert_id
    r = requests.post(starting_url)
    raw_data = r.text
    raw_data = json.loads(re.findall(r'rawData =.*', raw_data)[0][10:-2])
    open("text.json", "w").write(json.dumps(raw_data, indent=4, sort_keys=True))
    latest_alert_id = raw_data['MaxId']
    for x in raw_data['SportGroups']:
        leagues[x['SportGroupName']] = x['SportId']
        rleagues[x['SportId']] = x['SportGroupName']
    for x in raw_data['SportEvents']:
        process_event(SportEvent(x))


get_starting_data()


def post_urgent():
    data = request(True)


def post_message():
    if latest_alert_id == 0:
        post_urgent()
    data = request()
    open("message.json", "w").write(json.dumps(data, indent=4, sort_keys=True))
    process_messages(data)


while True:
    time.sleep(5)
    post_message()

