from pyspark import SparkContext, SparkConf

FILE_PATH = "../SparkScala/SparkScala/1800.csv"

def parseLine(line):
    fields = line.split(",")
    stationId, entryType, temperature = fields[0], fields[2], fields[3]
    temperature = float(temperature) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationId, entryType, temperature)

if __name__ == "__main__":
    sc = SparkContext("local[*]", "TemperatureFilter")
    sc.setLogLevel("ERROR")

    lines = sc.textFile(FILE_PATH)
    rdd = lines.map(lambda line: parseLine(line))

    # keep every line containing "TMIN" (min temperature)
    minTempLines = rdd.filter(lambda line: line[1] == "TMIN")

    # no longer need "TMIN" field
    # (key, value) = (identifier, temperature)
    stationMinTemps = minTempLines.map(lambda line: (line[0], float(line[2])))

    # get the min temp of stations
    stationMinTemp = stationMinTemps.reduceByKey(lambda x, y: min(x, y)).collect()

    print(stationMinTemp)


