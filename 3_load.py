#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

from os.path import dirname, basename, abspath
from datetime import datetime
import logging
import sys
import argparse
import glob
import pandas as pd

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError
from elasticsearch.helpers import bulk, streaming_bulk

def create_senti_index(client, index):
    # we will use user on several places
    user_mapping = {
      'properties': {
        'name': {
          'type': 'text',
          'fields': {
            'keyword': {'type': 'keyword'},
          }
        }
      }
    }

    create_index_body = {
      'settings': {
        # just one shard, no replicas for testing
        'number_of_shards': 1,
        'number_of_replicas': 0
      },
      'mappings': {
        'doc': {
          'properties': {
            'monitor_id': {'type': 'keyword'},
            'monitor_name': {'type': 'text'},
            'brand': {'type': 'keyword'},
            'product': {'type': 'keyword'},
            'geo': {'type': 'keyword'},
            'category': {'type': 'keyword'},
            'feature': {'type': 'keyword'},
            'source': {'type': 'keyword'},
            'date': {'type': 'date'},
            'num': {'type': 'integer'},
            'positive': {'type': 'integer'},
            'neutral': {'type': 'integer'},
            'negative': {'type': 'integer'}
          }
        }
      }
    }

    # create empty index
    try:
        client.indices.create(
            index=index,
            body=create_index_body,
        )
    except TransportError as e:
        # ignore already existing index
        if e.error == 'index_already_exists_exception':
            pass
        elif e.error == 'resource_already_exists_exception':
            pass
        else:
            raise

def parse_csv(path):

    path = path + "/*.csv"
    for fname in glob.glob(path):
        df = pd.read_csv(fname, sep="|")
        # NaN value error
        df = df.fillna('')
        for index, row in df.iterrows():
            yield {
                'monitor_id': row['monitor_id'],
                'monitor_name': row['monitor_name'],
                'brand': row['brand'],
                'product': row['product'],
                'geo': row['geo'],
                'category': row['category'],
                'feature': row['feature'],
                'source': row['source'],
                'date': row['date'],
                'num': row['num'],
                'positive': row['positive'],
                'neutral': row['neutral'],
                'negative': row['negative'],
            }

def load_csv(client, path=None, index='test_index'):
    path = dirname(dirname(abspath(__file__))) if path is None else path

    create_senti_index(client, index)

    for ok, result in streaming_bulk(
            client,
            parse_csv(path),
            index=index,
            doc_type='doc',
            chunk_size=50 
        ):
        action, result = result.popitem()
        doc_id = '/%s/doc/%s' % (index, result['_id'])
        if not ok:
            print('Failed to %s document %s: %r' % (action, doc_id, result))


if __name__ == '__main__':
    # get trace logger and set level
    tracer = logging.getLogger('elasticsearch.trace')
    tracer.setLevel(logging.INFO)
    tracer.addHandler(logging.FileHandler('./es_trace.log'))

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-H", "--host",
        action="store",
        default="localhost:9200",
        help="The elasticsearch host you wish to connect too. (Default: localhost:9200)")
    parser.add_argument(
        "-p", "--path",
        action="store",
        default=None,
        help="Path to csv files (Default: None")

    args = parser.parse_args()

    es = Elasticsearch(args.host)
    
    load_csv(es, path=args.path)

    # we can now make docs visible for searching
    es.indices.refresh(index='test_index')

    # and now we can count the documents
    print(es.count(index='test_index')['count'], 'documents in index')

