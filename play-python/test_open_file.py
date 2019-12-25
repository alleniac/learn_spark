
MOVIE_NAMES = "../SparkScala/ml-100k/u.item"

movieNames = {}

with open(MOVIE_NAMES, "r", encoding = "ISO-8859-1") as f:
    lines = f.readlines()
    for line in lines:
        sub = line.split("|")
        id, name = sub[0], sub[1]
        movieNames[id] = name

print(movieNames)    

