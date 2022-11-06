import sys
import csv
import numpy as np

from pyspark import SparkConf, SparkContext

def parse_csv_data(path):
    try:
        csv_file = open(path, 'r')
        csv_reader = csv.reader(csv_file, delimiter=',')
        points = list()
        for row in csv_reader:
            points.append([float(i) for i in row])
        return points
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))
        exit(1)

def parse_vector(line):
    return np.array([float(x) for x in line.split(' ')])

def closest_point(p, centers):
    index = 0
    closest = float("+inf")
    for i in range(len(centers)):
        temp_distance = np.sum((p - centers[i]) ** 2)
        if temp_distance < closest:
            closest = temp_distance
            index = i
    return index

if __name__ == "__main__":

    if len(sys.argv) != 5:
        print("Usage: k-means.py <file_input> <file_output> <k> <converge_distance>")
        exit(-1)

    conf = SparkConf() \
        .setMaster("local") \
        .setAppName("k-means app!") \

    sc = SparkContext(conf = conf)

    data = sc.parallelize(parse_csv_data(sys.argv[1]));

    k = int(sys.argv[3])

    converge_distance = float(sys.argv[4])
    k_points = data.takeSample(False, k, 1)

    temp_distance = 1.0

    while temp_distance > converge_distance:
        closest = data.map(
            lambda p: (closest_point(p, k_points), (p, 1)))
        point_stats = closest.reduceByKey(
            lambda p1_c1, p2_c2: (p1_c1[0] + p2_c2[0], p1_c1[1] + p2_c2[1]))
        new_points = point_stats.map(
            lambda st: (st[0], st[1][0] / st[1][1])).collect()

        temp_distance = sum(np.sum((k_points[i_k] - p) ** 2) for (i_k, p) in new_points)

        for (i_k, p) in new_points:
            k_points[i_k] = p
        
        k_points.persist()


    with open(sys.argv[2], "w") as result_file:
        result_file.write(str(k_points))

    
    

