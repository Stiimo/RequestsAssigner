# -*- coding: utf-8 -*-

from datetime import date, datetime, timedelta
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


def get_empty_requests(cursor):
    cursor.execute("SELECT `requestID`, `destinationPointID`, `warehousePointID`,"
                   "`deliveryDate`, `boxQty`, `weight`, `volume`"
                   "FROM `requests_tmp` WHERE (`requestStatusID`=%s OR `requestStatusID`=%s) AND `routeListID` IS NULL",
                   ("CHECK_PASSED", "READY"))
    return cursor.fetchall()



def get_route_lists(cursor):
    cursor.execute("SELECT routeListID, routeID FROM route_lists_tmp WHERE status=%s", ["CREATED"])
    return cursor.fetchall()


def get_possible_routes(cursor, request):
    cursor.execute("SELECT routeID FROM route_points WHERE pointID=%s", [request[1]])
    route_ids = cursor.fetchall()
    routes = list()
    for route_id in route_ids:
        cursor.execute("SELECT * FROM route_points WHERE routeID=%s and pointID=%s",
                       [route_id[0], request[2]])
        if len(cursor.fetchall()):
            cursor.execute("SELECT firstPointArrivalTime, daysOfWeek FROM routes WHERE routeID=%s", [route_id[0]])
            data = cursor.fetchone()
            departure = data[0]
            days = data[1]
            routes.append((int(route_id[0]), departure, days))
    routes.sort(key=lambda x: nearest(x[1], x[2]))
    return routes


def in_time(cursor, request, route):
    pass


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

    def get_urgent_requests(self, cursor):
        cursor.execute("SELECT requestID, boxQty, weight, volume FROM requests_tmp WHERE routeListID=%s",
                       [self.route_list_id])
        self.urgent = cursor.fetchall()

    def filter_requests(self, cursor, empty_requests):
        for item in list(empty_requests):
            routes = get_possible_routes(cursor, item)
            if self.route_id in routes:
                self.to_urgent.append(item)
                empty_requests.remove(item)

    def assign_requests(self, cursor, connection, route_id):
        np.random.seed(int(datetime.now().timestamp()))
        if self.route_list_id == -1:
            departure = date.today()+timedelta(days=nearest(self.departure, self.days))
            id_external = ''.join([str(np.random.randint(0, 10)) for _ in range(8)])
            list_number = ''.join([str(np.random.randint(0, 10)) for _ in range(10)])
            cursor.execute("INSERT INTO route_lists (routeListIDExternal, dataSourceID, routeListNumber,"
                           "creationDate, departureDate, status, routeID) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                           [id_external, "LOGIST_1C", list_number, date.today().strftime("%Y-%m-%d"),
                            departure.strftime("%Y-%m-%d"), "CREATED", route_id])
            connection.commit()
            cursor.execute("SELECT routeListID FROM route_lists WHERE routeID=%s", [route_id])
            res = cursor.fetchall()
            self.route_list_id = res[-1][0]
        for item in sorted(self.to_urgent, key=lambda x: x[4], reverse=True):
            if self.check_capacities(item):
                self.urgent.append(item)
                self.decrease_capacities(item)
                cursor.execute("UPDATE requests SET routeListID=%s WHERE requestID=%s", [self.route_list_id, item[0]])
                connection.commit()
        for item in sorted(self.assigned, key=lambda x: x[4], reverse=True):
            if self.check_capacities(item):
                self.decrease_capacities(item)
            else:
                self.assigned.remove(item)
                cursor.execute("UPDATE requests SET routeListID=%s WHERE requestID=%s", [self.route_list_id, None])
                connection.commit()
        for item in sorted(self.to_assign, key=lambda x: x[4], reverse=True):
            if self.check_capacities(item):
                self.assigned.append(item)
                self.decrease_capacities(item)
                cursor.execute("UPDATE requests SET routeListID=%s WHERE requestID=%s", [self.route_list_id, item[0]])
                connection.commit()

    def check_capacities(self, item):
        return self.weight - (item[5] or 0) >= 0 and self.volume - (item[6] or 0) >= 0 and self.boxQty - (item[4] or 0) >= 0

    def decrease_capacities(self, item):
        self.boxQty -= (item[4] or 0)
        self.weight -= (item[5] or 0)
        self.volume -= (item[6] or 0)

    def calculate_capacities(self):
        weight = 0
        volume = 0
        boxQty = 0
        for item in self.urgent:
            boxQty += (item[1] or 0)
            weight += (item[2] or 0)
            volume += (item[3] or 0)
        self.weight = 1 - weight
        self.volume = 1 - volume
        self.boxQty = 100 - boxQty

    def set_departure(self, cursor):
        cursor.execute("SELECT firstPointArrivalTime, daysOfWeek FROM routes WHERE routeID=%s", [self.route_id])
        data = cursor.fetchone()
        self.departure = data[0]
        self.days = data[1]
