import json
from functools import reduce
from pymongo import MongoClient, UpdateOne
from typing import Iterable, Dict, Callable, List, Any, TypeVar, Mapping
from .classes import Point

__all__= ['add_point', 'add_points', 'add_fields']

with open('config.json') as config_file:
    config = json.loads(config_file.read())

client = MongoClient(config['database_uri'])
db = client['lifeplusDb']
collection = db['data']

# server_version = tuple(client.server_info()['version'].split('.'))
# print(f'{server_version=}')

batch: List[UpdateOne] = []

def bulk_write(func: Callable[..., UpdateOne]):

    def wrapper(*args, **kwargs):
        global batch
        update = func(*args, **kwargs)
        batch.append(update)
        if len(batch) > 1000:
            print('Unloading batch')
            r = collection.bulk_write(batch)
            result_messages = r.bulk_api_result
            assert not result_messages['writeConcernErrors'], result_messages['writeConcernErrors']
            assert not result_messages['writeErrors'], result_messages['writeErrors']
            batch = []

    return wrapper

T = TypeVar('T')

def make_nested_dict(base_dict: Mapping[Any, T], parent_name: str) -> Dict[str, T]:
    try:
        assert not any('.' in k for k in base_dict.keys())
    except TypeError: # Key is an int, as in a Point.as_raw_dict()
        pass
    return {f'{parent_name}.{k}': v for k, v in base_dict.items()}


def prepare_pt(pt: Point):
    return pt.to_utc().as_raw_dict()


@bulk_write
def add_points(uid: str, points: Iterable[Point]):
    prepared_pts = reduce(lambda acc, pt: {**acc, **prepare_pt(pt)}, points, {})
    return UpdateOne(
        {"_id": uid},
        {"$set": make_nested_dict(prepared_pts, 'points')},
        upsert=True
    )


@bulk_write
def add_point(uid: str, point: Point):
    return UpdateOne(
        {"_id": uid},
        {"$set": make_nested_dict(prepare_pt(point), 'points')},
        upsert=True
    )


@bulk_write
def add_fields(uid: str, fields: Mapping):
    return UpdateOne(
        {"_id": uid},
        {"$set": make_nested_dict(fields, 'fields')},
        upsert=True
    )