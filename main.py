import pandas as pd
from random import seed
from random import randint
import numpy as np

df_dict = {}
df_dict_index = {}
df_dict_size = {}
aggr_df = {}

def generatedf():
    for i in range(5):
        rows_len = randint(2000, 2500)
        df = pd.DataFrame(np.random.uniform(-60, 130, size=(rows_len, 2)), columns=['RelTime', 'example'+str(i)])
        df = df.sort_values('RelTime').reset_index(drop=True)
        df_dict['example'+str(i)] = df


def findStartTimeInDF():
    startTime = min([df.iloc[0]['RelTime'] for df in df_dict.values()])
    return startTime


def init_df_dict_index():
    for k in df_dict.keys():
        df_dict_index[k] = 0

def init_df_dict_size():
    for k in df_dict.keys():
        df_dict_size[k] = len(df_dict[k].index) - 1

def init_df_dict_agg():
    for k in df_dict.keys():
        aggr_df[k] = pd.DataFrame()

def main():
    generatedf()
    init_df_dict_index()
    init_df_dict_size()
    init_df_dict_agg()
    startTime = findStartTimeInDF()
    interval = 0.1
    aggr_func = ['sum', 'max', 'mean']
    # start from startTime and every iteration add intreval
    # find batches of rows of interval in all dfs and aggregate
    # use a index for every df in dict

    time_index = startTime
    stop = False
    while not stop:
        time_index+=interval
        stop = True
        #iterate over all df by their index
        t_arr = []
        for k, df in df_dict.items():
            # sub_df = pd.dataframe()
            if(df_dict_index[k] < df_dict_size[k]): #check if reach the end already in this df
                start_i = df_dict_index[k]
                while(df_dict_index[k] < df_dict_size[k] and #didn't reach end of df
                      df.iloc[df_dict_index[k]]['RelTime'] < time_index): #row is in batch
                    df_dict_index[k]+=1

                # agg
                # pd.DataFrame(data, index=[0])
                t_arr.append(df_dict[k].iloc[df_dict_index[k]-1]['RelTime'])

        for k,df in df_dict.items():
            if (df_dict_index[k] <= df_dict_size[k]):
                data = {'RelTime': min(t_arr)} #set the reltime to be the min reltime of all df in current batch
                for func_name in aggr_func:
                    data[k + '_' + func_name] = df.iloc[start_i:df_dict_index[k], 1].agg(func_name)
                if (aggr_df[k].empty):
                    aggr_df[k] = pd.DataFrame(data, index=[0])
                else:
                    aggr_df[k] = aggr_df[k].append(pd.DataFrame(data, index=[0]))
            df_dict_index[k]+=1

        stop &= (df_dict_index[k] == df_dict_size[k])

    print("hello")



if __name__ == "__main__":
    main()