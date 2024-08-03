import argparse
import os
import torch
import numpy as np


from cupy_numba.delta_cupy import compute_all as compute_all_GPU_delta
from cupy_numba.delta_numba import compute_all as compute_all_CPU_delta
from cupy_numba.vanna_cupy import compute_all_GPU as compute_all_GPU_vanna
from cupy_numba.vanna_cupy import compute_all as compute_all_CPU_vanna


def compute_all(args, book, prices):
    # breakpoint()
    if args.mode == "vanna":
        if torch.cuda.is_available() and torch.cuda.device_count()>0:
            result=compute_all_GPU_vanna(book, prices)
        else:
            result=compute_all_CPU_vanna(book, prices, args.proc)
    elif args.mode == "delta":

        if torch.cuda.is_available() and torch.cuda.device_count()>0:
            print(f'Cuda is available: {torch.cuda.is_available()}')
            print(f'Cuda device count: {torch.cuda.device_count()}')
            result=compute_all_GPU_delta(book, prices)
        else:
            result=compute_all_CPU_delta(book, prices, args.proc)
    else:
        print("mode not implemented")
        return None
    return result

if __name__ == '__main__':


    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", help="Calculation mode", choices=['delta', 'vanna'], default='delta')
    parser.add_argument("--proc", help="Number of processors to use", default=32)
    args = parser.parse_args()

    num_processors = os.cpu_count()

    print("Number of processors available:", num_processors)


    args.proc = num_processors
    args.mode = 'vanna'

    breakpoint()


    # Load data    
    # book = np.load(os.path.join(os.pardir, 'files', 'book.npy'), allow_pickle= True)
    # book = book[:1024]
    # book[:,2][book[:,2] == 0 ] = 1e-10 # this could be added within the compute_all function
    #
    # prices = np.load(os.path.join(os.pardir, 'files', 'prices.npy'), allow_pickle= True)

    # TODO: Remove after backtest

    # dates = od.read_table('select distinct(date(effective_date)) from results.mm_books').values.tolist()
    #
    # for date in dates:
    #     date = date[0].strftime('%Y-%m-%d')
    #     print(date)
    #     df_book = od.get_sepcific_book(date, 'SPX')
    #     breakpoint()

        #result = compute_all(args, book, prices)
    # print(result[:5,:5])
    # breakpoint()

    #

    #args.proc = os.cpu_count()
    #args.proc = 'delta'
    #args.proc = 'vanna'
    
