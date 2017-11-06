#!/usr/bin/python

import csv
import datetime
import math
import MySQLdb
import second

HOST = 'localhost'
USER = 'root'
PASSWORD = '123123'
DATABASE = 'mysql'
PATH = 'raw_data.csv'
INTERVAL = 900

class TaskDatabaseManager(object):

    def __init__(self, host, user, passwd, db):
        self.db = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db)
        self.sql = self.db.cursor()

    def create_table(self, table='result'):
        self.sql.execute(
            "CREATE TABLE %s ("
            "  timeframe_start datetime NOT NULL,"
            "  api_name varchar(50) NOT NULL,"
            "  http_method varchar(10) NOT NULL,"
            "  count_http_code_5xx int NOT NULL,"
            "  is_anomaly bool NOT NULL"
            ");" % table)
        return self.sql.fetchall()

    def drop_table(self, table='result'):
        self.sql.execute(
            "DROP TABLE %s;" % table)
        return self.sql.fetchall()

    def insert(self, timeframe_start, api_name, http_method, count_http_code_5xx, is_anomaly, table='result'):
        self.sql.execute(
            "INSERT INTO %s (timeframe_start, api_name, http_method, count_http_code_5xx, is_anomaly)"
            "VALUES ('%s', '%s', '%s', %s, %s)" % (table, timeframe_start, api_name, http_method, count_http_code_5xx, is_anomaly))
        return self.sql.fetchall()

    def select(self, table='result'):
        self.sql.execute(
            "SELECT * FROM %s" % table)
        return self.sql.fetchall()

    def commit(self):
        self.db.commit()

    def __del__(self):
        self.db.close()


def to_timestamp(date):
    split = date.split(',')
    int_part = int(datetime.datetime.strptime(split[0], "%Y-%m-%d %H:%M:%S").strftime("%s"))
    float_part = float(split[1]) / 1000
    return float(int_part + float_part)


def to_date(timestamp):
    split = (repr(timestamp)).split('.')
    int_part = datetime.datetime.fromtimestamp(int(split[0])).strftime("%Y-%m-%d %H:%M:%S")
    float_part = str(split[1])
    return int_part + "." + float_part

def create_dict(keys):
    result = {}
    for key in keys:
        result.update({key: [0, False]})
    return result

def get_data(path):
    result = {}
    pairs = set()
    with open(path) as f:
        csvfile = csv.reader(f)
    	for row in csvfile:
        	if row[3][0] == '5':
            		if to_timestamp(row[0]) not in result:
                		result.update({to_timestamp(row[0]): [(row[1], row[2])]})
            		else:
                		result[to_timestamp(row[0])].append((row[1], row[2]))
            		pairs.add((row[1], row[2]))
    return result, pairs

def parse():
	dataset = {}
	data, pairs = get_data(PATH)
	timestamps = sorted(data.keys())
	current = 0
	length = len(timestamps)
	temp = create_dict(pairs)
	timeframe_start = timestamps[0]
	while current < length:
		if timestamps[current] < timeframe_start + INTERVAL:
			items = data[timestamps[current]]
			for item in items:
				temp[item][0] += 1
				current += 1
		else:
			dataset.update({timeframe_start: temp})
			temp = create_dict(pairs)
			timeframe_start += INTERVAL
	return dataset, pairs



dataset, pairs = parse()

for pair in pairs:
    distribution = second.get_distribution(dataset, pair)
    anomaly = second.get_anomaly(distribution)
    if len(anomaly) != 0:
        for item in dataset:
            if dataset[item][pair][0] in anomaly:
                dataset[item][pair][1] = True

dm = TaskDatabaseManager(HOST, USER, PASSWORD, DATABASE)
dm.drop_table()
dm.create_table()
for timestamp in dataset:
    for item in dataset[timestamp]:
        if dataset[timestamp][item][0] != 0:
            dm.insert(to_date(timestamp), item[0], item[1], dataset[timestamp][item][0],dataset[timestamp][item][1])
dm.commit()
