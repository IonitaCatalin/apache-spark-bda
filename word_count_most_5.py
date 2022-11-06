from os import listdir
from os.path import isfile, join
from os import getcwd
from pyspark import SparkConf, SparkContext


def get_files_from_dir(dir_path):
    return  [join(dir_path,f) for f in listdir(dir_path) if isfile(join(dir_path, f))]

if __name__ == '__main__':
    conf = SparkConf() \
        .setMaster("local") \
        .setAppName("Word Count App!") \

    current_dir_path = getcwd()
    
    # result_path = '/Users/catalin/Documents/bda/apache-spark-bda/shakespeare/results'
    result_path = join(current_dir_path, 'shakespeare/results')
    main_dir_path = join(current_dir_path, 'shakespeare');

    sc = SparkContext(conf = conf)

    for file_path in get_files_from_dir(main_dir_path):

        counts = sc.textFile(file_path) \
            .flatMap(lambda line: line.split()) \
            .filter(lambda word: len(word) <= 5) \
            .map(lambda word: (word,1)) \
            .reduceByKey(lambda v1,v2: v1+v2) \
            .collect()


        with open(join(result_path, file_path.split('/')[-1] ,'counts_at_most_5.out'), 'w') as result_file:
            result_file.write(str(counts) + '\n')
        
        