from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    sc = SparkContext("local[*]", "RatingsCounter")
    sc.setLogLevel("ERROR")

    lines = sc.textFile("../SparkScala/ml-100k/u.data")
    ratings = lines.map(lambda line: line.split("\t")[2])
    results = ratings.countByValue()

    sortedResults = sorted(results.items(), key=lambda x: x[0])

    for res in sortedResults:
        print(res)
