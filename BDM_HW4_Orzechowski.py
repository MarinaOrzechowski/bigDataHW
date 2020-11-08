from pyspark import SparkContext
import sys

# get rid of headers, filter bad-quality data,
# return ((year, product, company), 1)
def extractData(partId, records):
    if partId==0:
        next(records)
    import csv
    reader = csv.reader(records)
    for row in reader:
        if len(row)>7 and row[0][:4].isdigit():
            yield ((row[1].lower(), row[0][:4], row[7].lower()),1)

# calculate highest percentage (rounded to the nearest whole number)
# of total complaints filed against one company for that product and year
def findMaxPercent(record):
    product, year = record[0]
    total_compl, max_compl, companies = record[1]
    max_percent = int(max_compl*100.0/total_compl+0.5)
    return ((product, year), (total_compl, companies,max_percent))

# format output to a string separated by commas
def formatToString(record):
    product, year = record[0]
    if ',' in product:
        product = '"' + product + '"'
    return ','.join((product, year, str(record[1][0]), str(record[1][1]),str(record[1][2])))


if __name__=='__main__':
    fileName = '/data/share/bdm/complaints.csv' if len(sys.argv)<2 else sys.argv[1]
    outputDirectory = sys.argv[2] if len(sys.argv)>2 else 'output'
    sc = SparkContext()
    rdd = sc.textFile(fileName)
    result = rdd.mapPartitionsWithIndex(extractData)\
                  .reduceByKey(lambda x,y: x+y)\
                  .map(lambda x: (x[0][:2], (x[1],x[1], 1)))\
                  .reduceByKey(lambda x, y : ((x[0]+y[0]), max(x[1],y[1]), x[2]+y[2]))\
                  .map(findMaxPercent)\
                  .sortByKey()\
                  .map(formatToString)\
                  .collect()
    sc.parallelize(result).saveAsTextFile(outputDirectory)
