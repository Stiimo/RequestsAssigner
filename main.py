#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from tqdm import tqdm
import mysql.connector as mc
from route import *
from atomic_id import *


if __name__ == "__main__":
    connection = mc.connect(host="localhost", user="root", db="", password="aftR179Kp", port=8889)
    cursor = connection.cursor()
    cursor.execute("USE `transmaster_transport_db`")
    print("Connection established")

    cursor.execute("SELECT MAX(routeListIDExternal) FROM route_lists")
    atomic_route_list_id_external = AtomicId(cursor.fetchone()[0] or "00000000")
    cursor.execute("SELECT MAX(routeListNumber) FROM route_lists")
    atomic_route_list_number = AtomicId(cursor.fetchone()[0] or "0000000000")

    empty_requests = get_empty_requests(connection, cursor)
    print("Got {} empty requests".format(len(empty_requests)))
    route_lists = get_route_lists(cursor)
    print("Got {} route lists".format(len(route_lists)))
    routes = dict()
    print("Creating routes from route lists")
    for item in tqdm(route_lists):
        routes[item[1]] = Route(cursor, item[1], item[0])
        routes[item[1]].filter_requests(cursor, empty_requests)
        routes[item[1]].get_requests(cursor)
        routes[item[1]].calculate_capacities(cursor)
    print("Creating routes without route lists")
    for item in tqdm(empty_requests):
        possible_routes = get_possible_routes(cursor, item)
        if len(possible_routes) == 0:
            continue
        route_id = possible_routes[0][0]
        if route_id not in routes.keys():
            routes[route_id] = Route(cursor, route_id)
            routes[route_id].calculate_capacities(cursor)
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
    print("Asssigning...")
    for id, route in tqdm(routes.items()):
        route.assign_requests(cursor, connection, id, atomic_route_list_id_external, atomic_route_list_number)
    print("Updating statuses")
    for route in tqdm(routes.values()):
        route.update_status(cursor, connection)

    connection.close()
