import csv
import datetime
import math
import MySQLdb


def get_distribution(dataset, pair):
    result = {}
    for data in dataset:
        amount = dataset[data][pair][0]
        if amount in result:
            result[amount] += 1
        elif amount != 0:
            result.update({amount: 1})
    return result

def get_anomaly(distribution):
	result = set()
	M = 0
	M_sq = 0
	for item in distribution:
		M += item * distribution[item]
		M_sq +=  item * item * distribution[item]
	M /= 1.0*len(distribution)
	M_sq /= 1.0*len(distribution)
	sigma = math.sqrt(math.fabs(M_sq - M * M))
	for item in distribution:
		if item > M + 3 * sigma or item < M - 3 * sigma:
			result.add(item)
	return result



