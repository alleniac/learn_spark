from pyspark import SparkContext
import re

FILE_PATH = "../learn_spark/SparkScala/SparkScala/book.txt"

"""
1. Simply split the lines with whitespace and then flatMap them
2. Apply regular expression to ignore punctuations and upper/lower case
3. Keep RDD to enhance the performance
"""
if __name__ == "__main__":
    sc = SparkContext("local[*]", "TemperatureFilter")
    sc.setLogLevel("ERROR")

    lines = sc.textFile(FILE_PATH)

    # ignore punctuations and convert everything to lower case
    words = lines.flatMap(lambda x: re.split("\\W+", x)).map(lambda x: x.lower())

    # keep this RDD
    # each item is a (word, count) pair
    # revert them to (count, word) pair and then sort by key
    words = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0]))
    words = words.sortByKey().map(lambda x: (x[1], x[0])).collect()

    print(words)