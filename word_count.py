from os import listdir
from os.path import isfile, join
from pyspark import SparkConf, SparkContext


def get_files_from_dir(dir_path):
    return  [join(dir_path,f) for f in listdir(dir_path) if isfile(join(dir_path, f))]

if __name__ == '__main__':
    conf = SparkConf() \
        .setMaster("local") \
        .setAppName("Word Count App!") \
    
    result_path = '/home/catalinm/Documents/studying/bda/shakespeare/results'

    sc = SparkContext(conf = conf)

    for file_path in get_files_from_dir('/home/catalinm/Documents/studying/bda/shakespeare'):

        counts = sc.textFile(file_path) \
            .flatMap(lambda line: line.split()) \
            .map(lambda word: (word,1)) \
            .reduceByKey(lambda v1,v2: v1 + v2) \
            .collect()

        with open(join(result_path, join(result_path, file_path.split('/')[-1] ,'counts.out')), 'w') as result_file:
            result_file.write(str(counts) + '\n')
        
        