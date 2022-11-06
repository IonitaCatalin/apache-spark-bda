import csv
import random
import math
import sys
import time
import matplotlib.pyplot as plt

from copy import deepcopy
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


def init_k_centroids(sc, k_value, dataset):
    maximums = list()
    minimums = list()
    for index in range(0, len(dataset[0])):
        maximums.append(max([element[index] for element in dataset]))
        minimums.append(min([element[index] for element in dataset]))

    centroids = [[] for i in range(0, k)]

    for i in range(0, k_value):
        for j in range(0, len(dataset[0])):
            centroids[i].append(round(random.uniform(minimums[j], maximums[j]), 2))

    return centroids


def init_k_plus_centroids(dataset):
    centroids = list()
    centroids.append(dataset[random.randrange(0, len(dataset))])
    for i in range(k - 1):
        distances = []
        for j in range(len(dataset)):
            point = dataset[j]
            distance = sys.maxsize
            for j in range(len(centroids)):
                temporary = euclidean_distance(point, centroids[j])
                distance = min(distance, temporary)
            distances.append(distance)
        centroids.append(dataset[distances.index(max(distances))])

    return centroids


def update_centroids(centroids, clustered_items):
    for count, item in enumerate(clustered_items):
        if item:
            centroids[count] = [round(sum(i) / len(item), 2) for i in zip(*item)]


def euclidean_distance(p, q):
    sum_of_sq = 0
    for i in range(len(p)):
        sum_of_sq += (p[i] - q[i]) ** 2
    return math.sqrt(sum_of_sq)


def compute_J(clusters, centroids):
    value = 0
    for count, cluster in enumerate(clusters):
        difference = 0
        for item in cluster:
            difference = difference + sum([item[i] - centroids[count][i] for i in range(0, len(item))])
        value = value + difference * difference
    return value


def k_means_algorithm(sc, k_value, dataset, max_iterations, distance_function):
    previous_centroids = None
    current_iteration = 0
    centroids = init_k_centroids(sc, k_value, dataset)

    while current_iteration in range(0, max_iterations) and previous_centroids != centroids:
        clusters = classify(centroids, dataset, distance_function)
        print('Iterations:', current_iteration)
        print('Clusters:', *clusters, sep='\n')

        previous_centroids = deepcopy(centroids)
        update_centroids(centroids, clusters)

        print('Current centroid:', centroids)
        current_iteration = current_iteration + 1



def classify(centroids, items, distance_function):
    clusters = [[] for i in range(len(centroids))]
    for item in items:
        minimum = sys.maxsize
        cluster = -1
        for count, centroid in enumerate(centroids):
            distance = distance_function(item, centroid)
            if distance < minimum:
                minimum = distance
                cluster = count
        clusters[cluster].append(item)
    return clusters


if __name__ == '__main__':

    if len(sys.argv) <= 2: 
        print('Usage: k-means.py [INPUT_FILE] [OUTPUT_FILE] [K_VALUE = 3] [ITERATION_NUMBER = 100]');
        exit(1);
    
    input_file_path = str(sys.argv[1])
    output_file_path = str(sys.argv[2])

    k = 3 if len(sys.argv) <= 3 else sys.argv[3]

    iterations = 100 if len(sys.argv) <= 4 else sys.argv[4]

    conf = SparkConf() \
        .setMaster("local") \
        .setAppName("Word Count App!")
    
    sc = SparkContext(conf = conf)
    
    dataset = sc.parallelize(parse_csv_data(input_file_path))

    k_means_algorithm(sc, k, dataset, iterations, euclidean_distance);

    start_time = time.time()
    print("--- Program ended in:%s seconds ---" % round(time.time() - start_time, 2))