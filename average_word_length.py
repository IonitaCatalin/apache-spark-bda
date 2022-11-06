from os import listdir
from os.path import isfile, join
from pyspark import SparkConf, SparkContext


def get_files_from_dir(dir_path):
    return  [join(dir_path,f) for f in listdir(dir_path) if isfile(join(dir_path, f))]

if __name__ == '__main__':
    conf = SparkConf() \
        .setMaster("local") \
        .setAppName("Average word count!") \
    
    result_path = '/home/catalinm/Documents/studying/bda/shakespeare/results'

    sc = SparkContext(conf = conf)

    for file_path in get_files_from_dir('/home/catalinm/Documents/studying/bda/shakespeare'):

        counts = sc.textFile(file_path) \
            .flatMap(lambda line: line.split()) \
            .map(lambda word: (word[0],1)) \
            .reduceByKey(lambda v1, v2: v1 + v2) \
        
        lengths = sc.textFile(file_path) \
            .flatMap(lambda line: line.split()) \
            .map(lambda word: (word[0], len(word))) \
            .reduceByKey(lambda v1, v2: (v1 + v2)) \

        counts = {pair[0]:pair[1] for pair in counts}
        lengths = {pair[0]:pair[1] for pair in lengths}

        averages = {}

        for key in lengths.keys():
            averages[key] = lengths[key] / counts[key];

        with open(join(result_path, join(result_path, file_path.split('/')[-1] ,'averages.out')), 'w') as result_file:
            result_file.write(str(joined))
        
        