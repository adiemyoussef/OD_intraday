import numpy as np

from numba import jit, njit
from scipy.stats import norm
from math import fabs, erf, erfc, exp

from multiprocessing import Pool

FAST_MATH = False

@njit(cache=True, fastmath=FAST_MATH)
def ndtr_numba(a : np.float64):
    NPY_SQRT1_2 = 1.0/ np.sqrt(2.0)

    if (np.isnan(a)):
        return np.nan

    x = a * NPY_SQRT1_2
    z = fabs(x)

    if (z < NPY_SQRT1_2):
        y = 0.5 * erf(x) + 0.5
    else:
        y = 0.5 * erfc(z)
        if (x > 0):
            y = 1.0 - y

    return y

@njit(cache=True, fastmath=FAST_MATH)
def d1( s : np.float64, k : np.float64, t : np.float64, r : np.float64, v : np.float64):
    return (np.log(s/(k+1e-18)) + (r + v * v / 2) * t / 365) / (v * np.sqrt(t / 365)+1e-18)

@njit(cache=True, fastmath=FAST_MATH)
def delta( flag, s, k, t, r, v):
    d_1 = d1(s, k, t, r, v)
    #return norm.cdf(d_1)-flag
    return ndtr_numba(d_1)-flag

@njit(cache=True, fastmath=FAST_MATH)
def compute(contract0 , contract1 : np.float64, contract2 : np.float64, contract3 : np.float64, contract4 : np.float64, list_prices:np.ndarray[np.float64]):
    """
    Objectif:
    :param
    :return
    """
    list_deltas_prices = []
    for price in list_prices:
        list_deltas_dte = []
        for dte in contract3:
            #print(contract0, price, contract1, dte, 0.04, contract2)
            d = delta(contract0, price, contract1, dte, 0.055, contract2)

            # multiplier par le net position
            net_delta = d * contract4
            list_deltas_dte.append(net_delta)
            #print(price + dte)

        list_deltas_prices.append(list_deltas_dte)
    return list_deltas_prices

def pool_sol(book, list_prices, num_processes):
    with Pool(num_processes) as pool:
        async_results = [pool.apply_async(compute, args=(np.int32(b[0]=="P"), np.float64(b[1]), np.float64(b[2]), np.array(b[3], dtype=np.float64), np.float64(b[4]), list_prices)) for b in book]
        results = [ar.get() for ar in async_results]
        deltas_arr = np.array(results).sum(axis=0)

        return deltas_arr

def compute_all(book, list_prices, num_processes=32):
    """
    Objectif:
    :param
    :return
    """
    print("---------- COMPUTE ----------")
    list_prices = np.array(list_prices)

    #deltas_arr = dask_sol(book, list_prices,num_processes)
    deltas_arr = pool_sol(book, list_prices, num_processes)
    return deltas_arr