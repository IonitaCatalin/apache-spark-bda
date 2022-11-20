from os import listdir
from os.path import isfile, join
from os import getcwd
from pyspark import SparkConf, SparkContext


def get_files_from_dir(dir_path):
    return  [join(dir_path,f) for f in listdir(dir_path) if isfile(join(dir_path, f))]

if __name__ == '__main__':
    conf = SparkConf() \
        .setMaster("local") \
        .setAppName("Average word count!") \
    
    current_dir_path = '/Users/catalin/Documents/bda/apache-spark-bda'
    
    result_path = join(current_dir_path, 'shakespeare/results')
    main_dir_path = join(current_dir_path, 'shakespeare');

    sc = SparkContext(conf = conf)

    for file_path in get_files_from_dir(main_dir_path):

        averages = sc.textFile(file_path) \
            .flatMap(lambda line: line.split()) \
            .map(lambda word: (word[0], len(word))) \
            .groupByKey() \
            .map(lambda tuple: (tuple[0], sum(tuple[1]) / len(tuple[1]))) \
            .collect()

        with open(join(result_path, join(result_path, file_path.split('/')[-1] ,'averages.out')), 'w') as result_file:
            result_file.write(str(averages))
        
        