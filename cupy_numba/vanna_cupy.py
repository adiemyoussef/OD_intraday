
from datetime import datetime, timedelta, time as t
from numba import jit, njit
from multiprocessing import Pool
from math import exp, sqrt, log, pi
import logging
import pandas_market_calendars as mcal
import torch
import numpy as np
import matplotlib.pyplot as plt
from random import choice, uniform

# Constantes
FAST_MATH = False
RISK_FREE_RATE = 0.055


# Check if CUDA is available via PyTorch and import CuPy accordingly
try:
    import torch
    cuda_available = torch.cuda.is_available()
except ImportError:
    cuda_available = False
    print("PyTorch is not installed. CUDA check cannot be performed.")

if cuda_available:
    try:
        import cupy as cp
        print("CuPy imported successfully.")
    except ImportError:
        cuda_available = False
        print("CuPy is not installed, even though CUDA is available.")

# Define a placeholder for the CuPy kernel function
vanna_kernel = None
if cuda_available:
    vanna_kernel = cp.RawKernel(
    r"""
    const double pi = 3.14159265358979323846;
    extern "C" __device__ double calc_d1(double s, double k, double T, double r, double v){
        return (log(s/(k+1e-12)) + (r + v * v / 2) * T) / (v * sqrt(T)+1e-12);
    }

    extern "C" __device__ double norm_pdf(double x){
        return exp(-x * x / 2) / sqrt(2 * pi);
    }

    extern "C" __device__ double calculate_vanna(double S, double K, double t, double sigma, double option_type){
        double RISK_FREE_RATE = 0.055;
        double T = t / 365.;
        double d1_value = calc_d1(S, K, T, RISK_FREE_RATE, sigma);  // Using T (years) here
        double N_prime_d1 = norm_pdf(d1_value);
        double d1_denominator = (log(S / K) + (RISK_FREE_RATE + 0.5 * sigma * sigma) * T) / (sigma * sigma) - T;
        double sqrt_T = sqrt(T);
        double vanna;

        if (option_type == 0){
            vanna = N_prime_d1 * (d1_denominator * sqrt_T);
        } else if (option_type == 1){
            vanna = -N_prime_d1 * (d1_denominator * sqrt_T);
        } else {
            vanna = -1e10;  // Error case
        }

        return vanna;
    }

    extern "C" __global__ void vanna_kernel(
        const double* __restrict__ price_list,
        const double* __restrict__ arr0,
        const double* __restrict__ arr1,
        const double* __restrict__ arr2,
        const double* __restrict__ arr3,
        const double* __restrict__ arr4,
        double* sol,
        int* sizes0
    )
    {
        unsigned int thread_id = blockIdx.x * blockDim.x + threadIdx.x;

        if(blockIdx.x >= sizes0[0]) return;

        const int M = sizes0[1];
        const int price_len = sizes0[2];

        int counter = threadIdx.x;
        while(counter < M * price_len){
            unsigned int price_id = counter / M;
            unsigned int M_id = counter % M;
            if(M_id >= M || price_id >= price_len) break; // No more job to do

            double t = arr3[blockIdx.x * M + M_id];
            double net_vanna = 0.0;
            if(t > 0.0){
                double vanna = calculate_vanna(price_list[price_id], arr1[blockIdx.x], t, arr2[blockIdx.x], arr0[blockIdx.x]);

                // Multiply by the net position
                net_vanna = vanna * arr4[blockIdx.x];
            }
            sol[blockIdx.x * M * price_len + counter] = net_vanna;
            counter += blockDim.x;
        }
    }
    """, 'vanna_kernel')


def compute_all_GPU(book, list_prices):
    """
    Objectif:
    :param
    :return
    """
    print("---------- COMPUTE ----------")

    N = len(book)
    M = len(book[0][3])
    len_prices = len(list_prices)

    result = cp.zeros(N * len_prices * M).astype(cp.double)
    sizes = cp.array([N, M, len_prices, 0]).astype(cp.int32)

    assert(N>=1024) # N must be larger than blockdim!
    vanna_kernel((N,), (1024,),
                    (
                        cp.array(list_prices, dtype=cp.double),
                        cp.array([r[0] == 'P' for r in book], dtype=cp.double),
                        cp.array([r[1] for r in book], dtype=cp.double),
                        cp.array([r[2] for r in book], dtype=cp.double),
                        cp.array(np.array([r[3] for r in book]).flatten(), dtype=cp.double),
                        cp.array([r[4] for r in book], dtype=cp.double),
                        result,
                        sizes))

    vannas_arr = result.reshape((N, len_prices, M)).sum(axis=0)
    return cp.asnumpy(vannas_arr)


@njit(cache=True, fastmath=FAST_MATH)
def d1(s : np.float64, k : np.float64, T : np.float64, r : np.float64, v : np.float64):
    """
    Objective: Calculate the d1 component of the Black-Scholes option pricing model.

    Parameters
    ----------
    s: float
        Price of the underlying asset.
    k: float
        Strike price of the option.
    T: float
        Time to expiration (in years).
    r: float
        Risk-free interest rate.
    v: float
        Volatility of the underlying asset.

    Returns
    -------
    The d1 value as part of the Black-Scholes formula.
    """

    return (np.log(s / (k + 1e-18)) + (r + v * v / 2) * T/365) / (v * np.sqrt(T/365) + 1e-18)

@njit(cache=True, fastmath=FAST_MATH)
def norm_pdf(x):
    """
    Objective: Calculate the probability density function of a standard normal distribution.

    Parameters
    ----------
    x: float
        Value for which to calculate the density.

    Returns
    -------
    float
        Probability density at x in a standard normal distribution.
    """
    return exp(-x**2 / 2) / sqrt(2 * pi)

@njit(cache=True, fastmath=FAST_MATH)
def calculate_vanna(S: np.float64, K:np.float64, t:np.float64 ,sigma:np.float64 , option_type:np.int32):
    """
    Objective: Calculate the vanna of an option, which measures the sensitivity of the
               option's vega to changes in the underlying asset price, or the change of delta
               with regard to the fixed implied volatility.

    Parameters
    ----------
    S: float
        Current stock price.
    K: float
        Strike price of the option.
    t: float
        Time till expiration (in days).
    sigma: float
        Volatility of the underlying asset.
    option_type: int
        Type of the option (0 for call, 1 for put).

    Returns
    -------
    float
        The vanna of the option.
    """
    # Transform days till expiration to year till expiration
    T = t/365

    d1 = (np.log(S / (K + 1e-18)) + (RISK_FREE_RATE + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T)+ 1e-18)
    N_prime_d1 = norm_pdf(d1)
    d1_denominator = (np.log(S / (K + 1e-18)) + (RISK_FREE_RATE + 0.5 * sigma ** 2) * T) / (sigma ** 2) - T
    sqrt_T = sqrt(T)


    # if option_type == 0:
    #     vanna = N_prime_d1 * (d1_denominator * sqrt_T)
    # elif option_type == 1:
    #     vanna = -N_prime_d1 * (d1_denominator * sqrt_T)
    # else:
    #     # Returning a large negative number as an indicator of error
    #     return -1e10
    #
    # return vanna

    vanna = N_prime_d1 * (d1_denominator * sqrt_T)
    return vanna if option_type == 0 else -vanna  # Assuming negative vanna for puts

@njit(cache=True, fastmath=FAST_MATH)
def compute(flag: np.int32, strike: np.float64, iv: np.float64, dte_list: np.ndarray[np.float64], net_position: np.float64,list_prices: np.ndarray[np.float64]):
    """
    Objective: Compute the vanna for different prices and days to expiration, scaled by net position.

    Parameters
    ----------
    flag: int
        Option type indicator (0 for call, 1 for put).
    strike: float
        Strike price of the option.
    iv: float
        Implied volatility.
    net_position: float
        Net position in the option.
    dte_list: list
        List of days to expiration.
    list_prices: list
        List of underlying asset prices.

    Returns
    -------
    list
        A list of vanna values for each price and expiration date, scaled by the net position.
    """

    list_vanna_prices = []
    for price in list_prices:
        list_vanna_dte = []

        for dte in dte_list:
            #calculate_vanna(S: np.float64, K: np.float64, t: np.float64, sigma: np.float64, option_type: np.int32)
            vanna = calculate_vanna(price, strike, dte, iv, flag)
            # multiplier par le net position
            net_vanna = vanna * net_position
            list_vanna_dte.append(net_vanna)

        list_vanna_prices.append(list_vanna_dte)
    return list_vanna_prices

def pool_sol(book, list_prices, num_processes):
    """
    Objective: Calculate the aggregate vanna for an options book using multiprocessing.

    Parameters
    ----------
    book: list
        A collection of options contracts.
    list_prices: list
        List of underlying asset prices.
    num_processes: int
        Number of processes to use in the pool.

    Returns
    -------
    numpy.ndarray
        Aggregate vanna/delta for the options book.
    """

    book_test = book [:10]
    test = [(np.int32(b[0] == "P"), np.float64(b[1]), np.float64(b[2]), np.array(b[3], dtype=np.float64), np.float64(b[4]),list_prices) for b in book_test]

    logging.info(f"Pool Solution with {num_processes} processes")
    with Pool(num_processes) as pool:

        async_results = [pool.apply_async(compute, args=(np.int32(b[0]=="P"), np.float64(b[1]), np.float64(b[2]), np.array(b[3], dtype=np.float64), np.float64(b[4]), list_prices)) for b in book]
        results = [ar.get() for ar in async_results]
        results_arr = np.array(results).sum(axis=0)

        return results_arr

def compute_all(book, list_prices, num_processes=32):
    """
    Objective: Compute the aggregate vanna for an options book using the pool_sol multiprocessing method.

    Parameters
    ----------
    book: list
        A collection of options contracts.
    list_prices: list
        List of underlying asset prices.
    num_processes: int, optional
        Number of processes for parallel computation, default is 32.

    Returns
    -------
    numpy.ndarray
        Aggregate vanna for the options book.
    """
    logging.info("---------- COMPUTE ----------")
    list_prices = np.array(list_prices)
    deltas_arr = pool_sol(book, list_prices, num_processes)
    return deltas_arr

def random_option_parameters():
    """Generate random option parameters."""
    iv = uniform(0.1, 0.55)  # Random IV between 10% and 50%
    dte = uniform(1, 30)  # Random DTE between 20 and 30 days
    strike = uniform(90, 110)  # Random strike price between $90 and $110
    option_type = choice(['P', 'C'])
    return iv, dte, strike, option_type

def plot_vanna(stock_prices, vanna_values, title):
    """Plot Vanna against Stock Prices."""
    plt.figure(figsize=(10, 6))
    plt.plot(stock_prices, vanna_values, label='Vanna')
    plt.xlabel('Stock Price')
    plt.ylabel('Vanna')
    plt.title(title)
    plt.legend()
    plt.grid(True)
    plt.show()

def verify_vanna_caluclation():
    # Redefine the setup for a single combined plot instead of subplots
    plt.figure(figsize=(20, 10))

    # Generate and plot for 8 different sets of option parameters on the same graph
    for i in range(8):
        iv, dte, strike, option_type = random_option_parameters()
        #breakpoint()
        stock_prices = np.linspace(80, 120, 100)  # Stock prices from $80 to $120
        vanna_values = [calculate_vanna(price, strike, dte, iv, (option_type=="P")) for price in stock_prices]

        if option_type == 'P':
            type_of_option = "Call"
        else:
            type_of_option = "Put"

        title = f'IV={iv:.2f}, DTE={dte:.0f}d, Strike=${strike:.2f}, {type_of_option}'
        plt.plot(stock_prices, vanna_values, label=title)

    plt.xlabel('Stock Price')
    plt.ylabel('Vanna')
    plt.title('Vanna vs Stock Price for Different Option Parameters')
    plt.legend(loc='upper right', fontsize=8)
    plt.grid(True)
    plt.show()

if __name__ == "__main__":
    nyse = mcal.get_calendar('NYSE')
    cuda = torch.cuda.is_available()

    # Verify calculate_vanna()
    # verify_vanna_caluclation()

    # breakpoint()

    book = np.load('data/book.npy', allow_pickle= True)
    book = book[:1024]
    book[:,2][book[:,2] == 0 ] = 1e-10 # this could be added within the compute_all function

    prices = np.load('data/prices.npy', allow_pickle= True)

    result = compute_all(book, prices, num_processes=8)

    result2 = compute_all_GPU(book, prices)

    print(result.shape, result2.shape)
    #print("Mean absolute error: ", np.mean(np.abs(result-result2)))
    print(f"Mean absolute error: {np.mean(np.abs(result-result2))}")
    print(f"Max abs error: {np.max(np.abs(result-result2))}")
    print(result[:5,:5])
    print(result2[:5,:5])

    #breakpoint()