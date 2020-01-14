import pandas as pd
from random import seed
from random import randint
import numpy as np

df_dict = {}
df_dict_index = {}

def generatedf():
    for i in range(5):
        rows_len = randint(2000, 2500)
        df = pd.DataFrame(np.random.uniform(-60, 130, size=(rows_len, 2)), columns=['RelTime', 'example'+str(i)])
        df = df.sort_values('RelTime').reset_index(drop=True)
        df_dict['example'+str(i)] = df


def findStartTimeInDF():
    pass


def main():
    generatedf()
    startTime = findStartTimeInDF()
    interval = 0.1

    # start from startTime and every iteration add intreval
    # find batches of rows of interval in all dfs and aggregate
    # use a index for every df in dict


    print("hello")



if __name__ == "__main__":
    main()