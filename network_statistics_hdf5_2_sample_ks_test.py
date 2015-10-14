"""
    Given a CSV file that is a collection of values over some interval of time,
    that also contains the columns: Application, Source, Destination, Some Value,
    and Time

    This script aggregates samples identified by a unique ID over a length of time
    and performs a two sample Kolmogrov Smirnov Test on each sample against all
    other samples returning an n x n matrix of p-values where n is the
    number of samples

    To spice things up, this script utilizes threads and the hdf5 data model
    to more efficiently process large CSV files
"""
__author__ = 'Nathan Edwards'

import pandas
import numpy
import sys
import csv
import os
from scipy.stats import ks_2samp
from ast import literal_eval
import multiprocessing as mp

def CDF(array, time_series):
    cumulative = numpy.cumsum(array)
    return [cumulative]

def worker(rowsx, dataframe, dictionary, id_q, res_q):
    for y, rowsy in dataframe.iterrows():
        ks_samp = ks_2samp(rowsx['CDF'][0] , rowsy['CDF'][0])[1]
        if ks_samp > .9:
                if rowsx.to_dict().values()[1] not in dictionary:
                    id_q.put({rowsx.to_dict().values()[1] : rowsx.to_dict().values()[0]})
                if rowsy.to_dict().values()[1] not in dictionary:
                    id_q.put({rowsy.to_dict().values()[1] : rowsy.to_dict().values()[0]})
                res_q.put({'unique_id_x' : rowsx['unique_id'] , 'unique_id_y' : rowsy['unique_id'] , 'p-value' : ks_samp})

def res_writer_listener(res_q):
    f = open('Kolmogorov_Smirnov_results.csv', 'wb')
    writer = csv.DictWriter(f, fieldnames = [('unique_id_x'),('unique_id_y'), ('p-value')], delimiter = ',')
    writer.writeheader()
    while 1:
        line = res_q.get()
        if line == 'kill':
            break
        writer.writerow(line)
    f.close()

def id_writer_listener(id_q, dictionary):
    f = open('unique_ids.csv', 'wb')
    writer = csv.DictWriter(f, fieldnames = [('unique_id'),('CDF')], delimiter = ',')
    writer.writeheader()
    while 1:
        pair = id_q.get()
        if pair == 'kill':
            break
        if pair is not None:
            k, v = pair.items()[0]
            if k not in dictionary:
                writer.writerow({'unique_id' : k, 'CDF' : v})
                dictionary.update(pair)
    f.close()

def main():
    # Load csv
    data_df = pandas.read_csv(sys.argv[1])
    # options
    pandas.set_option('display.max_columns', 7)
    # drop rows with null values
    data_df = data_df.dropna()

    # Generate unique id values from concatenated row values
    data_df['unique_id'] = data_df['Application'].astype(str) + '_' + \
                             data_df['Source'].astype(str) +  '_' + \
                             data_df['Destination'].astype(str)
    # rename
    data_df = data_df.rename(columns={'Time': 'timestamp'})
    data_df = data_df.rename(columns={'Some Value': 'size'})

    # Convert times to epoch time
    data_df['timestamp'] = pandas.to_datetime(data_df['timestamp']).astype(numpy.int64)

    # Transient data frame
    trans_df = data_df[['unique_id', 'size', 'timestamp']]

    trans_df = trans_df.pivot_table(index='unique_id', columns='timestamp', values='size')
    trans_df = trans_df.fillna(0)

    # Get array of packet counts over time
    d = zip(trans_df.index.values, numpy.asarray(trans_df.values))
    packet_counts_df = pandas.DataFrame(d, columns=['unique_id','array'])

    # x coordinates time series
    time_series = list(trans_df.columns.values)

    # Generate function from discrete CDF
    packet_counts_df['CDF'] = packet_counts_df.apply(lambda col: CDF(col['array'], time_series), axis=1)
    cdf_df_tmp = packet_counts_df[['unique_id', 'CDF']]

    cdf_df_tmp.to_hdf('store.h5','data',format='fixed',mode='w')

    cdf_df = pandas.read_hdf('store.h5','data')
    if os.path.isfile('Kolmogorov_Smirnov_results.csv'):
        os.remove('Kolmogorov_Smirnov_results.csv')
    # Initializing Manager
    m = mp.Manager()
    res_q = m.Queue()
    id_q = m.Queue()
    uid_dict = dict()

    pool = mp.Pool(mp.cpu_count() - 2)

    # Listeners

    try:
        res_writer = mp.Process(target = res_writer_listener, args=(res_q,))
        res_writer.start()
        id_writer = mp.Process(target = id_writer_listener, args=(id_q, uid_dict))
        id_writer.start()
        for x, rowsx in cdf_df.iterrows():
            pool.apply_async(worker, (rowsx, cdf_df, uid_dict, id_q, res_q))
        pool.close()
        pool.join()
        id_q.put('kill')
        res_q.put('kill')
        res_writer.join()
        id_writer.join()


    finally:
        print 'done!'

if __name__ == "__main__":
   main()
