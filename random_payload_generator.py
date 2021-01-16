import os
import sys
import getopt
import random
import multiprocessing as mp

from tqdm import tqdm
import pandas as pd


def data_generator(_length):
    out = ""
    for _ in range(_length):
        out += hex(random.randint(1, 15)).split("x")[-1]
    return out


def data_dump(SAVE_PATH):
    while True:
        _length = random.randint(84, 3598)
        if _length % 2 == 0:
            break
    data = data_generator(_length)
    pd.to_pickle(data, SAVE_PATH)
    return 1


def main(argv):
    num_of_data = None
    SAVE_DIR = None
    process = os.cpu_count() // 2

    optlist, args = getopt.getopt(argv[1:], '', ['help', "num_data=", 'save_dir=', 'process='])
    for opt, arg in optlist:
        if opt == '--help':
            pass
        elif opt == '--num_data':
            num_of_data = int(arg)
        elif opt == "--save_dir":
            SAVE_DIR = arg
        elif opt == '--process':
            process = arg

    mp.freeze_support()
    id_list = [i for i in range(num_of_data)]
    SAVE_PATH_LIST = [os.path.join(SAVE_DIR, str(_id)) for _id in id_list]

    with mp.Pool(process) as pool:
        for _ in tqdm(pool.imap_unordered(data_dump, SAVE_PATH_LIST), total=num_of_data):
            pass


if __name__ == "__main__":
    main(sys.argv)