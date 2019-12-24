from pyspark import SparkContext

FILE_PATH = "../SparkScala/SparkScala/customer-orders.csv"

"""
`"""
if __name__ == "__main__":
    sc = SparkContext("local[*]", "CustomerSpending")
    sc.setLogLevel("ERROR")

    # (customerId, itemId, spending)
    lines = sc.textFile(FILE_PATH)

    # (customerId, spending)
    lines = lines.map(lambda line: line.split(",")).map(lambda line: (line[0], line[2]))
    
    lines = lines.reduceByKey(lambda x, y: float(x)+ float(y)).mapValues(lambda x: round(x, 2)).collect()

    print(lines)
    

