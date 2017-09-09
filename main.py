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
        routes[item[1]].filter_requests(cursor, empty_requests)
        routes[item[1]].get_requests(cursor)
        routes[item[1]].calculate_capacities(cursor)
    for item in empty_requests:
        possible_routes = get_possible_routes(cursor, item)
        route_id = possible_routes[0][0]
        if route_id not in routes.keys():
            routes[route_id] = Route(cursor, route_id)
            routes[route_id].calculate_capacities()
        routes_count = 0
        for route in possible_routes:
            days_count = 0
            for day in route[2]:
                today = date.today().weekday()
                start_time = datetime.now()
                days = day_to_int[day] - today
                start = timedelta(hours=start_time.hour, minutes=start_time.minute,
                                  seconds=start_time.second, microseconds=start_time.microsecond)
                if days < 0 or (days == 0 and route[1] - start < timedelta(hours=1)):
                    days += 7
                start_time += timedelta(days=days)
                start_time = datetime.combine(start_time.date(), time()) + route[1]
                if in_time(cursor, item, route[0], start_time):
                    days_count += 1
            routes_count += days_count
            if routes_count > 1:
                break
        if routes_count > 1:
            routes[route_id].to_assign.append(item)
        else:
            routes[route_id].to_urgent.append(item)
    for id, route in routes.items():
        route.assign_requests(cursor, connection, id)
    connection.close()
