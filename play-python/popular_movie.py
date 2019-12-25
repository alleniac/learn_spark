from pyspark import SparkContext

FILE_PATH = "../SparkScala/ml-100k/u.data"
MOVIE_NAMES = "../SparkScala/ml-100k/u.item"

def loadMovieNames():
    movieNames = {}
    
    # https://stackoverflow.com/questions/19699367/for-line-in-results-in-unicodedecodeerror-utf-8-codec-cant-decode-byte
    with open(MOVIE_NAMES, "r", encoding = "ISO-8859-1") as f:
        lines = f.readlines()
        for line in lines:
            sub = line.split("|")
            id, name = sub[0], sub[1]
            movieNames[id] = name

    return movieNames

if __name__ == "__main__":
    sc = SparkContext("local[*]", "CustomerSpending")
    sc.setLogLevel("ERROR")

    nameDict = sc.broadcast(loadMovieNames())

    # (userId, movieId, rating, timestamp)
    lines = sc.textFile(FILE_PATH)

    # (movieId, 1)
    lines = lines.map(lambda line: (line.split("\t")[1], 1))

    # ascending rankings of movies based on their number of ratings
    lines = lines \
            .reduceByKey(lambda x, y: x + y) \
            .map(lambda line: (line[1], line[0])) \
            .sortByKey() \
            .map(lambda line: (nameDict.value[line[1]], line[0])) \
            .collect()
    
    print(lines)
            
