#-*-coding:utf-8-*-


from pyspark import SparkConf, SparkContext
import sys

if __name__=="__main__":
    input = sys.argv[0]
    output = sys.argv[1]
    conf = SparkConf().setMaster("spark://log1:7077").setAppName("WordCount")
    sc = SparkContext(conf=conf)
    textFile = sc.textFile(input)
    counts = textFile.flatMap(lambda line:line.split(" ")).map(lambda word:(word,1)).reduceByKey(lambda a,b:a+b)
    counts.saveAsTextFile(output)