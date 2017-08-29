#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import mysql.connector as mc
from route import *

if __name__ == "__main__":
    connection = mc.connect(host="185.75.182.94", user="root", db="", password="aftR179Kp")
    cursor = connection.cursor()
    cursor.execute("USE `transmaster_transport_db`")
    empty_requests = get_empty_requests(cursor)
    route_lists = get_route_lists(cursor)
    routes = dict()
    for item in route_lists:
        routes[item[1]] = Route(cursor, item[1], item[0])
        routes[item[1]].filter_requests(empty_requests, cursor)
        routes[item[1]].get_urgent_requests(cursor)
        # routes[item[1]].get_assigned_requests(cursor)
        routes[item[1]].calculate_capacities()
    if len(empty_requests):
        for item in empty_requests:
            possible_routes = get_possible_routes(cursor, item)
            route_id = possible_routes[0][0]
            if route_id not in routes.keys():
                routes[route_id] = Route(cursor, route_id)
                routes[route_id].calculate_capacities()
            if len(possible_routes) > 1 and in_time(cursor, item, possible_routes[1]):
                routes[route_id].to_assign.append(item)
            else:
                routes[route_id].to_urgent.append(item)
    for id, route in routes.items():
        route.assign_requests(cursor, connection, id)
    connection.close()
