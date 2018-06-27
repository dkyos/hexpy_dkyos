#!/usr/bin/env python

'''
conda create -n py36 python=3.6 anaconda
alias 36penv='source activate py36'
'''

from hexpy import HexpySession, MonitorAPI, MetadataAPI
from pathlib import Path
import json

def save_json(json_obj, path: str = None) -> None:
    save_path = Path(path)
    if not save_path.exists():
        parent = save_path.parent
        if not parent.exists():
            save_path.parent.mkdir()
    with open(save_path, "w") as outfile:
        json.dump(json_obj, outfile)

def load_json(path: str = None):
    load_path = Path(path)
    if not load_path.exists():
        return None
    with open(load_path) as infile:
        return json.load(infile)

session = HexpySession.load_auth_from_file()
metadata_client = MetadataAPI(session)
monitor_client = MonitorAPI(session)

j_team = load_json("./meta_json/team_list.json")
if j_team is None: 
    j_team = metadata_client.team_list()
    save_json(j_team, "./meta_json/team_list.json")
    print(json.dumps(j_team))

for T in j_team['teams']:
    print(T['id'], T['name'])
    j_monitor = load_json("./meta_json/" + str(T['id']) + "_monitor.json")
    if j_monitor is None: 
        j_monitor = metadata_client.monitor_list(T['id'])
        save_json(j_monitor, "./meta_json/" + str(T['id']) + "_monitor.json")
        #print(json.dumps(j_monitor))

    for M in j_monitor['monitors']:
        details = monitor_client.details(M['id'])
        j_detail = load_json("./result_json/" + str(M['id']) + "_details.json")
        if j_detail is None: 
            save_json(details, ("./result_json/" + str(M['id']) + "_details.json"))

        start = details["resultsStart"]
        end = details["resultsEnd"]
        print(T['id'], ": ", M['id'], start, end, M['name'])
        
        j_senti = load_json("./result_json/" + str(M['id']) + "_senti.json")
        if j_senti is None: 
            j_senti = monitor_client.sentiment_and_categories(M['id'], start, end)
            save_json(j_senti, ("./result_json/" + str(M['id']) + "_senti.json"))

        j_volume = load_json("./result_json/" + str(M['id']) + "_volume.json")
        if j_volume is None: 
            # [HOURLY, DAILY, WEEKLY, MONTHLY]
            j_volume = monitor_client.volume(M['id'], start, end, "HOURLY")
            #j_volume = monitor_client.volume(M['id'], start, end, "DAILY")
            #j_volume = monitor_client.volume(M['id'], start, end, "WEEKLY")
            #j_volume = monitor_client.volume(M['id'], start, end, "MONTHLY")
            save_json(j_volume, ("./result_json/" + str(M['id']) + "_volume.json"))

        raise SystemExit


session.close()

