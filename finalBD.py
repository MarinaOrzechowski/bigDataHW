from pyspark import SparkContext
import sys



# return -1 if house number contains letters, or None
def houseNumToTuple(house):
    parts = house.split('-')
    if len(parts) > 1 and parts[0] and parts[1] and parts[0].isdigit() and parts[1].isdigit():
        return(int(parts[0]+parts[1]))
    elif len(parts) == 1 and parts[0].isdigit():
        return (int(parts[0])*100)
    else:
        return -1


# So our logic below is to use the partition index to check if we're hitting
# the header (aka the first partition). If so, we just skip the first row.
def countyNameToCode(name):
    vocab = {
             'MAN':1,'NY': 1,'MH':1,'MN':1,'NEWY':1,'NEW Y':1,
             'BRONX':2,'BX':2,'PBX':2,
             'BK': 3,'KING':3,'KINGS':3,'K':3,        
             'Q':4,'QU':4,'QN': 4,'QNS': 4,'QU': 4,'QUEEN': 4,
             'R':5,'RICHMOND': 5,
             '':None,
            }
    if name:
        return vocab[name]
    else:
        return None


def extractViolations(partId, records):
    if partId==0:
        next(records)
    import csv
    reader = csv.reader(records)
    for row in reader:
        (date, county, street, house) = (row[4][-4:], countyNameToCode(row[21]), row[24].lower(), houseNumToTuple(row[23]))

        if county and street and house:
            if house>0:
                yield (county, street, house, date),1


def extractStreetsInfo(partId, records):
    if partId==0:
        next(records)
    import csv
    reader = csv.reader(records)
    for row in reader:
        (physicalID, full_stree, st_label, borocode, odd_low, odd_high, even_low, even_high) = \
        (row[0], row[28].lower(), row[29].lower(), int(row[13]), houseNumToTuple(row[2]), houseNumToTuple(row[3]), houseNumToTuple(row[4]), \
         houseNumToTuple(row[5]))
        if odd_low or odd_high or even_low or even_high:
            if odd_low>0:
                if st_label == full_stree:
                    yield (borocode, full_stree), (physicalID, odd_low, odd_high, even_low, even_high)
                else:
                    yield (borocode, full_stree), (physicalID, odd_low, odd_high, even_low, even_high)
                    yield (borocode, st_label), (physicalID, odd_low, odd_high, even_low, even_high)


def selectIdYearFreq(houseYearFreqIdOddEvenRange):
    if not houseYearFreqIdOddEvenRange[0]:
        return((houseYearFreqIdOddEvenRange[1][0], '2013'), 0)
    
    houseYearFreq, idOddEvenRange = houseYearFreqIdOddEvenRange
    house, year, freq = houseYearFreq
    ID, oddLow, oddHigh, evenLow, evenHigh = idOddEvenRange
    return ((ID, year), freq)         


def appendTuples(prev, curr):
    path = 'myfile.txt'
    if isinstance(prev[0], str):
        return (prev, curr)
    else:
        flatten = [x for x in prev]

        if not isinstance(curr[0], str):
            curr = [x for x in curr]
            flatten.extend(curr)
        else:
            flatten.append(curr)

        return tuple(flatten)
    
#if house number matches, leave the freq. If not - freq is 0. If no house number: make key (none, 2013, 0) 
def matchHouseNums(record):
    if not record[0]:
        return ((record[1][0], '2013'), 0)
    houseYearFreq, idOddEvenRange = record
    house, year, freq = houseYearFreq
    ID, oddLow, oddHigh, evenLow, evenHigh = idOddEvenRange
    
    if house>0:
        if house % 2 == 0:
            if evenLow <= house <= evenHigh:
                return ((ID, year), freq)
        else:
            if oddLow<= house<= oddHigh:
                return ((ID, year), freq)
    return ((ID, year), 0)


def completeYears(record):
    ID, yearsGiven = record
    yearsGoal = [(str(i), 0) for i in range(2013, 2020)]
    if isinstance(yearsGiven[0], str):
        yearsGiven = [yearsGiven]
    else:
        yearsGiven = [x for x in yearsGiven]
    
    vocab = {}
    for pair in yearsGoal:
        vocab[pair[0]] = 0
    
    for pair in yearsGiven:
        if pair[0] in vocab:
            vocab[pair[0]] += pair[1]
    
    res = list((year, freq) for year, freq in vocab.items())
    res.sort()
    res = [str(freq) for year, freq in res]

    # format to comma separated string
    result = ','.join(res)
    result = ','.join((str(ID), result))
    return result


if __name__=='__main__':
    violationsFile = '/data/nyc_parking_violations/' 
    streetsFile = '/data/share/bdm/nyc_cscl.csv'
    outputDirectory = sys.argv[2] if len(sys.argv)>2 else 'output'

    sc = SparkContext()

    viol = sc.textFile(violationsFile, use_unicode=True).cache()
    violations = viol.mapPartitionsWithIndex(extractViolations).reduceByKey(lambda x,y: x+y)\
                     .map(lambda x: ((x[0][0], x[0][1]), (x[0][2], x[0][3], x[1])))

    streetsData = sc.textFile(streetsFile, use_unicode=True).cache()
    streets = streetsData.mapPartitionsWithIndex(extractStreetsInfo)

    mergedOnFullStree = violations.rightOuterJoin(streets).values()\
                                .map(matchHouseNums)\
                                .reduceByKey(lambda x, y: x+y)\
                                .map(lambda x: (x[0][0], (x[0][1], x[1])))\
                                .reduceByKey(appendTuples)\
                                .map(completeYears)

    sc.parallelize(mergedOnFullStree).saveAsTextFile(outputDirectory)

