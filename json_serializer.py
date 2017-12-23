# -*- coding: utf-8 -*-
import json
from route import Route
from datetime import datetime, timedelta


def encode_dict(d):
    d = dict(d)
    d["days"] = list(d["days"])
    return d


class RouteEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Route):
            d = dict(o.__dict__)
            d["days"] = list(d["days"])
            d["departure"] = (datetime(1, 1, 1) + d["departure"]).strftime("%H:%M:%S %d.%m.%Y")
            return {
                "_type": "Route",
                "value": d
            }
        if isinstance(o, timedelta):
            return {
                "_type": "datetime",
                "value": (datetime(1, 1, 1) + o).strftime("%H:%M:%S %d.%m.%Y")
            }
        if isinstance(o, datetime):
            return {
                "_type": "datetime",
                "value": o.strftime("%H:%M:%S %d.%m.%Y")
            }
        if isinstance(o, set):
            return {
                "_type": "set",
                "value": list(o)
            }
        return super(RouteEncoder, self).default(o)


class RouteDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, o):
        if "_type" not in o:
            return o
        type = o["_type"]
        if type == "Route":
            o = o["value"]
            tmp = Route(None, o["route_id"], o["route_list_id"])
            tmp.weight = o["weight"]
            tmp.volume = o["volume"]
            tmp.boxQty = o["boxQty"]
            tmp.urgent = o["urgent"]
            tmp.assigned = o["assigned"]
            tmp.to_urgent = o["to_urgent"]
            tmp.to_assign = o["to_assign"]
            tmp.departure = o["departure"]
            tmp.days = o["days"]
            return tmp
        if type == "datetime":
            return datetime.strptime(o["value"], "%H:%M:%S %d.%m.%Y")
        if type == "set":
            return set(o["value"])
        return o
