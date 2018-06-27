#!/usr/bin/env python

from pathlib import Path
import json
import pandas as pd
import re

def save_json(json_obj, path: str = None) -> None:
    save_path = Path(path)
    if not save_path.exists():
        parent = save_path.parent
        if not parent.exists():
            save_path.parent.mkdir()
    with open(save_path, "w") as outfile:
        json.dump(json_obj, outfile)

def save_csv(df, path: str = None) -> None:
    save_path = Path(path)
    if not save_path.exists():
        parent = save_path.parent
        if not parent.exists():
            save_path.parent.mkdir()
    df.to_csv(path, sep='|', index=False)

def load_json(path: str = None):
    load_path = Path(path)
    if not load_path.exists():
        return None
    with open(load_path) as infile:
        return json.load(infile)

j_team = load_json("./meta_json/team_list.json")
#if j_team is None: 

for T in j_team['teams']:
    print(T['id'], T['name'])
    j_monitor = load_json("./meta_json/" + str(T['id']) + "_monitor.json")
    #if j_monitor is None: 

    for M in j_monitor['monitors']:
        Brand = ""; Product = ""; Geo = ""; Category = ""; Feature = ""; Source = "";
        Positive = 0; Neutral = 0; Negative = 0;
        Date = ""; Time = "";

        tmp = re.split(': |- ', M['name'])
        if len(tmp) >= 1:
            for i, val in enumerate(tmp):
                val = val.strip()
                if val == 'Brand':
                    Brand = tmp[i+1].strip()
                    tmp2 = re.split(' ', Brand)
                    if tmp2[len(tmp2)-1] == 'Product':
                        Product = tmp[i+2].strip()
                        Brand = ' '.join(Brand.split(' ')[:-1])
                        Brand = Brand.strip()

                elif val == 'Product':
                    Product = tmp[i+1].strip()
                elif val == 'Geo':
                    Geo = tmp[i+1].strip()
                elif val == 'Category':
                    Category = tmp[i+1].strip()
                elif val == 'Feature':
                    Feature = tmp[i+1].strip()
                elif val == 'Source':
                    Source = tmp[i+1].strip()

        #if Brand is not "":
        #    print("%s" % (M['name']))
        #    print("     => %s|%s|%s|%s|%s|%s|" % (Brand, Product, Geo, Category, Feature, Source))
        #    print("")

        if Brand is "":
            continue

        j_senti = load_json("./result_json/" + str(M['id']) + "_senti.json")
        rows = []
        columns = ['monitor_id', "monitor_name"
            , 'brand', 'product', 'geo'
            , 'category', 'feature', 'source'
            , 'date', 'time', 'num'
            , 'positive', 'neutral', 'negative']
        for S in j_senti['results']:
            for C in S['categories']:
                if C['category'] == "Basic Positive":
                    positive = C['volume'];
                elif C['category'] == "Basic Neutral":
                    Neutral = C['volume'];
                elif C['category'] == "Basic Negative":
                    Negative = C['volume'];

            tmp = re.split('T', S['startDate'])
            if len(tmp) >= 2:
                Date = tmp[0].strip()
                Time = tmp[1].strip()

            #print("%d|%s|%s|%s|%s|%s|%s|%s|%s|%s|%d|%d|%d" % (M['id']
            #    , Brand, Product, Geo, Category, Feature, Source
            #    , Date, Time, S['numberOfDocuments']
            #    , positive, Neutral, Negative))

            rows.append([M['id'], M['name']
                , Brand, Product, Geo
                , Category, Feature, Source
                , Date, Time, S['numberOfDocuments']
                , positive, Neutral, Negative])

        df = pd.DataFrame(rows, columns=columns)
        print(df.head(3))
        save_csv(df, "./result_csv/" + str(M['id']) + "_senti.csv")

