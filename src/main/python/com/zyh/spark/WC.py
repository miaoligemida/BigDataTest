#-*-coding:utf-8-*-

from pyspark import SparkConf, SparkContext
import sys
import jieba
import findspark
findspark.init()

def split(line):
    words = jieba.cut(line.strip().split(" ")[1])
    ls = []
    for word in words:
        if len(word) > 1:
            ls.append(word)
    return ls

def combine(line):
    result = ""
    result += line[0] + "\t" + str(line[1])
    return result

def main(sc,input,output):
    text = sc.text(input)
    words = text.map(split).collect()
    count = sc.parallelize(words[0])
    count.map(lambda w:(w,1)).reduceByKey(lambda x,y:x+y).map(combine).sortByKey().saveAsTextFile(output)

if __name__=="__main__":
    input = sys.argv[0]
    output = sys.argv[1]
    conf = SparkConf().setAppName("WordCount").setMaster("spark://log1:7077")
    sc = SparkContext(conf=conf)
    main(sc,input,output)