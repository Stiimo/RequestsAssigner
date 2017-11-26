# -*- coding: utf-8 -*-
import json
from route import Route


class RouteEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Route):
            return {
                "_type": "Route",
                "value": o.__dict__
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
            tmp = Route(None, o["route_id"], o["route_list_id"])
            tmp.weight = o["weight"]
            self.volume = o["volume"]
            self.boxQty = o["boxQty"]
            self.urgent = o["urgent"]
            self.assigned = o["assigned"]
            self.to_urgent = o["to_urgent"]
            self.to_assign = o["to_assign"]
            self.departure = o["departure"]
            self.days = o["days"]
            return tmp
        return o


class SetEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, set()):
            return {
                "_type": "Route",
                "value": o.__dict__
            }
        return super(RouteEncoder, self).default(o)