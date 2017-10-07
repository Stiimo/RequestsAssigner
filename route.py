# -*- coding: utf-8 -*-

from datetime import date, datetime, timedelta, time
import numpy as np


day_to_int = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6
}
routeListIDExternal = ""
routeListNumber = ""


def increase(s):
    s = [c for c in s]
    for i in reversed(range(len(s))):
        d = 0
        if s[i] == '9':
            s[i] = 'A'
        elif s[i] == 'Z':
            s[i] = '0'
            d = 1
        else:
            s[i] = chr(ord(s[i])+1)
        if d == 0:
            break
    return ''.join(s)


def get_empty_requests(connection, cursor):
    cursor.execute("SELECT requestID, destinationPointID, deliveryDate, "
                   "boxQty, weight, volume, storage "
                   "FROM requests WHERE (requestStatusID=%s OR requestStatusID=%s) AND routeListID IS NULL",
                   ("CHECK_PASSED", "READY"))
    requests = cursor.fetchall()
    for i in range(len(requests)):
        requests[i] = list(requests[i])
    for request in list(requests):
        cursor.execute("SELECT point_id FROM storages_to_points WHERE storage=%s", [request[6]])
        try:
            request[6] = cursor.fetchone()[0]
            cursor.execute("UPDATE requests SET warehousePointID=%s WHERE requestID=%s",
                           [request[6], request[0]])
        except:
            requests.remove(request)
        if request[2] is None:
            cursor.execute("SELECT requestDate FROM requests WHERE requestID=%s", [request[0]])
            date = cursor.fetchone()[0]
            request[2] = datetime(date.year, date.month, date.day) + timedelta(days=2)
            cursor.execute("UPDATE requests SET deliveryDate=%s WHERE requestID=%s",
                           [request[2], request[0]])
        if request[3] is None:
            request[3] = 1 # TODO check
    connection.commit()
    return requests


def get_route_lists(cursor):
    cursor.execute("SELECT routeListID, routeID FROM route_lists WHERE status=%s", ["CREATED"])
    return cursor.fetchall()


def get_possible_routes(cursor, request):
    cursor.execute("SELECT routeId FROM delivery_route_points WHERE pointId=%s", [request[1]])
    route_ids = cursor.fetchall()
    routes = list()
    for route_id in route_ids:
        cursor.execute("SELECT * FROM route_points WHERE routeID=%s and pointID=%s",
                       [route_id[0], request[6]])
        if len(cursor.fetchall()):
            cursor.execute("SELECT firstPointArrivalTime, daysOfWeek FROM routes WHERE routeID=%s", [route_id[0]])
            data = cursor.fetchone()
            departure = data[0]
            days = data[1]
            routes.append((int(route_id[0]), departure, days))
    routes.sort(key=lambda x: nearest(x[1], x[2]))
    return routes


def in_time(cursor, request, route_id, cur_time):
    # TODO get time from table
    cur_time += timedelta(minutes=1440)
    return cur_time <= request[2]

def nearest(departure, days):
    today = date.today().weekday()
    now = datetime.now().time()
    now = timedelta(hours=now.hour, minutes=now.minute, seconds=now.second, microseconds=now.microsecond)
    ans = 7
    for i in days:
        cur = day_to_int[i]-today
        if cur < 0 or (cur == 0 and departure - now < timedelta(hours=1)):
            cur += 7
        if cur < ans:
            ans = cur
    return ans


class Route:
    def __init__(self, cursor, route_id, route_list_id=-1):
        self.route_id = route_id
        self.route_list_id = route_list_id or -1
        self.weight = 0
        self.volume = 0
        self.boxQty = 0
        self.urgent = list()
        self.assigned = list()
        self.to_urgent = list()
        self.to_assign = list()
        self.departure = None
        self.days = None
        self.set_departure(cursor)

    def get_requests(self, cursor):
        cursor.execute("SELECT requestID, destinationPointID, deliveryDate, "
                       "boxQty, weight, volume, warehousePointID, requestStatusID "
                       "FROM requests WHERE routeListID=%s",
                       [self.route_list_id])
        requests = cursor.fetchall()
        for item in requests:
            possible_routes = get_possible_routes(cursor, item)
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
                    start_time += timedelta(days=7)
                    if in_time(cursor, item, route[0], start_time):
                        days_count += 1
                routes_count += days_count
                if routes_count > 1:
                    break
            if routes_count > 1:
                self.assigned.append(item)
            else:
                self.urgent.append(item)

    def filter_requests(self, cursor, empty_requests):
        for item in list(empty_requests):
            possible_routes = get_possible_routes(cursor, item)
            routes = list(map(lambda x: x[0], possible_routes))
            if self.route_id in routes:
                empty_requests.remove(item)
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
                        start_time += timedelta(days=7)
                        if in_time(cursor, item, route[0], start_time):
                            days_count += 1
                    routes_count += days_count
                    if routes_count > 1:
                        break
                if routes_count > 1:
                    self.to_assign.append(item)
                else:
                    self.to_urgent.append(item)

    def assign_requests(self, cursor, connection, route_id, keys):
        np.random.seed(int(datetime.now().timestamp()))
        if self.route_list_id == -1:
            departure = date.today()+timedelta(days=nearest(self.departure, self.days))
            keys[0] = increase(keys[0])
            keys[1] = increase(keys[1])
            cursor.execute("INSERT INTO route_lists (routeListIDExternal, dataSourceID, routeListNumber, "
                           "creationDate, departureDate, status, routeID) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                           [keys[0], "LOGIST_1C", keys[1], date.today().strftime("%Y-%m-%d"),
                            departure.strftime("%Y-%m-%d"), "CREATED", route_id])
            connection.commit()
            cursor.execute("SELECT routeListID FROM route_lists WHERE routeID=%s", [route_id])
            res = cursor.fetchall()
            self.route_list_id = res[-1][0]
        for item in sorted(self.to_urgent, key=lambda x: x[3], reverse=True):
            if self.check_capacities(item):
                self.urgent.append(item)
                self.decrease_capacities(item)
                cursor.execute("UPDATE requests SET routeListID=%s "
                               "WHERE requestID=%s", [self.route_list_id, item[0]])
                connection.commit()
        for item in sorted(self.assigned, key=lambda x: x[3], reverse=True):
            if self.check_capacities(item):
                self.decrease_capacities(item)
            else:
                self.assigned.remove(item)
                cursor.execute("UPDATE requests SET routeListID=%s "
                               "WHERE requestID=%s", [None, item[0]])
                connection.commit()
        for item in sorted(self.to_assign, key=lambda x: x[3], reverse=True):
            if self.check_capacities(item):
                self.assigned.append(item)
                self.decrease_capacities(item)
                cursor.execute("UPDATE requests SET routeListID=%s "
                               "WHERE requestID=%s", [self.route_list_id, item[0]])
                connection.commit()

    def check_capacities(self, item):
        return self.boxQty - (item[3] or 0) >= 0 and self.weight - (item[4] or 0) >= 0 and self.volume - (item[5] or 0) >= 0

    def decrease_capacities(self, item):
        self.boxQty -= (item[3] or 0)
        self.weight -= (item[4] or 0)
        self.volume -= (item[5] or 0)

    def calculate_capacities(self, cursor):
        weight = 0
        volume = 0
        boxQty = 0
        for item in self.urgent:
            boxQty += (item[3] or 0)
            weight += (item[4] or 0)
            volume += (item[5] or 0)
        cursor.execute("SELECT boxQty, weight, volume FROM routes WHERE routeID=%s", [self.route_id])
        capacities = cursor.fetchone()
        self.boxQty = (capacities[0] or 1) - boxQty
        self.weight = (capacities[1] or 1) - weight
        self.volume = (capacities[2] or 1) - volume

    def set_departure(self, cursor):
        cursor.execute("SELECT firstPointArrivalTime, daysOfWeek FROM routes WHERE routeID=%s", [self.route_id])
        data = cursor.fetchone()
        self.departure = data[0]
        self.days = data[1]

    def update_status(self, cursor, connection):
        today = date.today().weekday()
        now = datetime.now().time()
        now = timedelta(hours=now.hour, minutes=now.minute, seconds=now.second, microseconds=now.microsecond)
        for i in self.days:
            if day_to_int[i] - today == 0 and self.departure - now < timedelta(hours=1):
                cursor.execute("UPDATE route_lists SET status=%s "
                               "WHERE routeListID=%s", ["APPROVED", self.route_list_id])
                cursor.execute("UPDATE requests SET requestStatusID=%s "
                               "WHERE routeListID=%s", ["TRANSPORTATION", self.route_list_id])
        connection.commit()
