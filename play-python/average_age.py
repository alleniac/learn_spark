from pyspark import SparkContext, SparkConf

FILE_PATH = "../SparkScala/SparkScala/fakefriends.csv"

"""
Input: A line of text containing (index, name, age, number of friends)
Return a (key, value) pair representing (age, number of friends)
"""
def parseLine(line):
    line = line.split(",")[-2:]
    return tuple(line)

if __name__ == "__main__":
    sc = SparkContext("local[*]", "AverageCalculator")
    sc.setLogLevel("ERROR")

    lines = sc.textFile(FILE_PATH)

    # convert each line to (age, numOfFriends) tuple
    rdd = lines.map(lambda line: parseLine(line))

    # (age, numOfFriends) => (age, (numOfFriends, 1))
    middleware = rdd.mapValues(lambda x: (x, 1))

    # (age, (numOfFriends, 1)) => (age, (numOfFriends, totalNumOfThisAge))
    middleware = middleware.reduceByKey(lambda x, y: (int(x[0]) + int(y[0]), x[1] + y[1]))

    # (age, (numOfFriends, totalNumOfThisAge)) => (age, averageNumOfFriendsOfThisAge)
    middleware = middleware.mapValues(lambda x: round(x[0] / x[1]))

    # sort it
    results = sorted(middleware.collect())
    
    for result in results:
        print(f"In average, users of age {result[0]} has {result[1]} friends")
    



