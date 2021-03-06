import time

from pyspark.sql import SparkSession

def calculate(fileName, totalFile):
    f = open("../tmp/hashFile/"+fileName,"r")
    dic = {}
    while True:
        line = f.readline()
        if line:
            dic[line] = dic.get(line, 0) + 1
        else:
            break
    f.close()
    cnt = 0
    d = sorted(dic.items(), key = lambda k:k[1], reverse= True)
    print(d)
    for i in d:
        cnt += 1
        if i[0] == "\n":
            cnt -= 1
            continue
        totalFile.write(str(i[0][:-2]) + " " + str(i[1])+"\n")
        if cnt == 100:
             break


def mainSpark(): #该方法仅为使用Spark的初次尝试
    print("Spark Start")
    f = open("../tmp/totalResultSpark.txt", "a")
    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .master("local[*]")\
        .getOrCreate()

    for i in range(1000):
        name = str(i) + ".txt"
        lines = spark.read.text("../tmp/hashFile/"+name).rdd.map(lambda r: r[0])
        counts = lines.flatMap(lambda x: x.split('\n')) \
            .map(lambda x: (x, 1)) \
            .reduceByKey(lambda x, y: x + y)
        output = counts.collect()
        for (URL, count) in output:
            f.write(URL+" "+str(count)+"\n")

    print("Spark End")

def main():
    f = open("../tmp/totalResult.txt","a")
    starttime = time.time()
    for i in range(1000):
        name = str(i) + ".txt"
        calculate(name, f)
    endtime = time.time()
    print("总花费时间是"+str(round(endtime - starttime, 2))+"seconds")
    f.close()


if __name__ == '__main__':
    main()
