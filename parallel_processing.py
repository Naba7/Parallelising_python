import multiprocessing as mp
import numpy as np
from time import time
import pandas as pd

print("Number of processors: ", mp.cpu_count())
np.random.RandomState(100)
arr = np.random.randint(0, 10, size=[200000, 5])
data = arr.tolist()
print(data[:5])

def howmany_within_range(row, minimum, maximum):
	count = 0
	for n in row:
		if minimum <= n <= maximum:
			count = count +1
	return count
results = []

for row in  data:
	results.append(howmany_within_range(row, minimum=4, maximum=8))

print(results[:10])

pool = mp.Pool(mp.cpu_count())

results = [pool.apply(howmany_within_range, args=(row, 4, 8)) for row in data]

pool.close()

print(results[:10])

def howmany_within_range_rowonly(row, minimum=4, maximum=8):
	count = 0
	for n in row:
		if minimum <= n <= maximum:
			count = count +1
	return count

pool = mp.Pool(mp.cpu_count())

results = pool.map(howmany_within_range_rowonly, [row for row in data])

pool.close()

print(results[:10])


pool = mp.Pool(mp.cpu_count())

results = pool.starmap(howmany_within_range, [(row, 4, 8) for row in data])

pool.close()

print(results[:10])


#Asynchronous processing

pool = mp.Pool(mp.cpu_count())

results = []

def howmany_within_range2(i, row, minimum, maximum):
    
    count = 0
    for n in row:
        if minimum <= n <= maximum:
            count = count + 1
    return (i, count)


def collect_result(result):
    global results
    results.append(result)



for i, row in enumerate(data):
    pool.apply_async(howmany_within_range2, args=(i, row, 4, 8), callback=collect_result)

   
pool.close()
pool.join()  

results.sort(key=lambda x: x[0])
results_final = [r for i, r in results]

print(results_final[:10])

pool = mp.Pool(mp.cpu_count())

results = []

result_objects = [pool.apply_async(howmany_within_range2, args=(i, row, 4, 8)) for i, row in enumerate(data)]

results = [r.get()[1] for r in result_objects]

pool.close()
pool.join()
print(results[:10])

# without callback

pool = mp.Pool(mp.cpu_count())

results = []

result_objects = [pool.apply_async(howmany_within_range2, args=(i, row, 4, 8)) for i, row in enumerate(data)]

results = [r.get()[1] for r in result_objects]

pool.close()
pool.join()
print(results[:10])

#pool_starmap_async()

pool = mp.Pool(mp.cpu_count())

results = []

results = pool.starmap_async(howmany_within_range2, [(i, row, 4, 8) for i, row in enumerate(data)]).get()

pool.close()
print(results[:10])

#parallelising_with_pandas

#row-wise and column-wise parallelisation
df = pd.DataFrame(np.random.randint(3, 10, size=[5, 2]))
print(df.head())

#Row-wise Parallelization

def hypotenuse(row):
    return round(row[1]**2 + row[2]**2, 2)**0.5

with mp.Pool(4) as pool:
    result = pool.imap(hypotenuse, df.itertuples(name=False), chunksize=10)
    output = [round(x, 2) for x in result]
print(output)

#Column-wise Parallelization

def sum_of_squares(column):
    return sum([i**2 for i in column[1]])

with mp.Pool(2) as pool:
    result = pool.imap(sum_of_squares, df.iteritems(), chunksize=10)
    output = [x for x in result]

print(output) 

#Data-Frame parallelization

from pathos.multiprocessing import ProcessingPool as Pool

df = pd.DataFrame(np.random.randint(3, 10, size=[500, 2]))

def func(df):
    return df.shape

cores=mp.cpu_count()

df_split = np.array_split(df, cores, axis=0)

pool = Pool(cores)

df_out = np.vstack(pool.map(func, df_split))

pool.close()
pool.join()
pool.clear()