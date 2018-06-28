#!/usr/bin/env python

'''
conda create -n py36 python=3.6 anaconda
alias 36penv='source activate py36'
'''

from hexpy import HexpySession, MonitorAPI, MetadataAPI
from pathlib import Path
import json
import datetime

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


j_countries = load_json("./meta_json/countries.json")
if j_countries is None: 
    j_countries = metadata_client.countries()
    save_json(j_countries, "./meta_json/countries.json")
    #print(json.dumps(j_countries))

j_team = load_json("./meta_json/team_list.json")
if j_team is None: 
    j_team = metadata_client.team_list()
    save_json(j_team, "./meta_json/team_list.json")
    #print(json.dumps(j_team))

#raise SystemExit

for T in j_team['teams']:
    print(T['id'], T['name'])
    j_monitor = load_json("./meta_json/" + str(T['id']) + "_monitor.json")
    if j_monitor is None: 
        j_monitor = metadata_client.monitor_list(T['id'])
        save_json(j_monitor, "./meta_json/" + str(T['id']) + "_monitor.json")
        #print(json.dumps(j_monitor))

    for M in j_monitor['monitors']:
        j_detail = load_json("./result_json/" + str(M['id']) + "_details.json")
        if j_detail is None: 
            j_detail = monitor_client.details(M['id'])
            save_json(j_detail, ("./result_json/" + str(M['id']) + "_details.json"))

        start = j_detail["resultsStart"]
        end = j_detail["resultsEnd"]
        print(T['id'], ": ", M['id'], start, end, M['name'])
        dt_start = datetime.datetime.strptime(start, "%Y-%m-%dT%H:%M:%S")
        dt_end = datetime.datetime.strptime(end, "%Y-%m-%dT%H:%M:%S")
        #print (dt_start)
        #print (dt_end)

        j_senti = load_json("./result_json/" + str(M['id']) + "_senti.json")
        if j_senti is None: 
            j_senti = monitor_client.sentiment_and_categories(M['id'], start, end)
            save_json(j_senti, ("./result_json/" + str(M['id']) + "_senti.json"))

        try:
            j_volume = load_json("./result_json/" + str(M['id']) + "_volume.json")
            if j_volume is None: 
                # [HOURLY, DAILY, WEEKLY, MONTHLY]
                j_volume = monitor_client.volume(M['id'], start, end, "DAILY")
                save_json(j_volume, ("./result_json/" + str(M['id']) + "_volume.json"))

            d = dt_start
            delta = datetime.timedelta(days=1)
            while d <= dt_end:
                start2 = d.strftime("%Y-%m-%d")
                d += delta
                end2 = d.strftime("%Y-%m-%d")
                print(T['id'], ": ", start2, end2)

                j_country = load_json("./result_json/" + str(M['id']) + "_" + start2 + "_volume_countries.json")
                if j_country is None: 
                    j_volume = monitor_client.countries(M['id'], start2, end2)
                    save_json(j_volume, ("./result_json/" + str(M['id']) + "_" + start2 + "_volume_countries.json"))

            #j_country = load_json("./result_json/" + str(M['id']) + "_volume_countries.json")
            #if j_country is None: 
            #    j_volume = monitor_client.countries(M['id'], start, end)
            #    save_json(j_volume, ("./result_json/" + str(M['id']) + "_volume_countries.json"))

        except Exception as e:
            pass



        raise SystemExit
        '''
        for S in j_detail['subfilters']:
            print(S['id'], " : ", S['name'])
            #print(S['id'], " : ", S['name'], " : ", S['parameters'])

            if S['name'].strip() != '(심화) Senti':
                continue

            j_senti = load_json("./result_json/" + str(M['id']) + "_" + str(S['id']) + "_senti.json")
            if j_senti is None: 
                j_senti = monitor_client.sentiment_and_categories(S['id'], start, end)
                save_json(j_senti, ("./result_json/" + str(M['id']) + "_" + str(S['id']) + "_senti.json"))

            try:
                j_volume = load_json("./result_json/" + str(M['id']) + "_" + str(S['id'])+ "_volume.json")
                if j_volume is None: 
                    # [HOURLY, DAILY, WEEKLY, MONTHLY]
                    j_volume = monitor_client.volume(S['id'], start, end, "DAILY")
                    save_json(j_volume, ("./result_json/" + str(M['id']) + "_" + str(S['id']) + "_volume.json"))
            except Exception as e:
                pass
        '''
        #raise SystemExit

session.close()


