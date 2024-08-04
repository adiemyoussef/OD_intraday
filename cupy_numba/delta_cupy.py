import os
import time
import argparse
import numpy as np
import findiff
import matplotlib.pyplot as plt

# Check if CUDA is available and CuPy can be imported
try:
    import torch
    use_cuda = torch.cuda.is_available()
except ImportError:
    use_cuda = False
    print("PyTorch is not installed. CUDA check cannot be performed.")

if use_cuda:
    try:
        import cupy as cp
        xp = cp
        print("Using CuPy for GPU acceleration.")
    except ImportError:
        use_cuda = False
        print("CuPy is not installed. Falling back to NumPy.")

if not use_cuda:
    xp = np
    print("Using NumPy. No GPU acceleration.")

# Now use `xp` instead of `cp` or `np` throughout your code.

# Replace `cp.RawKernel` with a conditional definition
GPU_func = None
if use_cuda:
    GPU_func = cp.RawKernel(
    r"""
    extern "C" __device__ double delta(double flag, double s, double k, double t, double r, double v){
        double d = (log(s/(k)) + (r + v * v / 2) * t / 365) / (v * sqrt(t / 365));
        return normcdf(d)-flag;
    }

    extern "C" __global__ void GPU_func(
        const double* __restrict__ price_list,
        const double* __restrict__ arr0,
        const double* __restrict__ arr1,
        const double* __restrict__ arr2,
        const double* __restrict__ arr3,
        const double* __restrict__ arr4,
        double* sol,
        //double* debug,
        int* sizes0)
    {
        //unsigned int thread_id = blockIdx.x * blockDim.x + threadIdx.x;

        if(blockIdx.x >= sizes0[0]) return;
        
        const int M = sizes0[1];
        const int price_len = sizes0[2];

        int counter = threadIdx.x;
        while(counter < M*price_len){
            unsigned int price_id = counter/M;
            unsigned int M_id = counter%M;
            if(M_id >= M || price_id >= price_len) break; // No more job to do
            
            double t = arr3[blockIdx.x*M + M_id];
            double net_delta = 0.0;
            if(t>0.0){
                double d = delta(arr0[blockIdx.x], price_list[price_id], arr1[blockIdx.x], t, 0.04, arr2[blockIdx.x]);

                // === Multiplier par le net position ===
                net_delta = d * arr4[blockIdx.x];
            }
            sol[blockIdx.x*M*price_len+counter] = net_delta;
            counter += blockDim.x;
        }
    }
    """, 'GPU_func')

def compile(N, M, len_prices):
    result = cp.zeros(N*len_prices*M).astype(cp.double)
    sizes = cp.array([N, M, len_prices, 0]).astype(cp.int32)
    
    thread_per_block = 1024

    GPU_func((N,),(thread_per_block,),
        (
        cp.zeros(len_prices, dtype=cp.double),
        cp.zeros(N ,dtype=cp.double),
        cp.zeros(N, dtype=cp.double),
        cp.zeros(N, dtype=cp.double),
        cp.zeros(N*M, dtype=cp.double),
        cp.zeros(N, dtype=cp.double),
        result,
        sizes))

def compute_all(book, list_prices):
    """
    Objectif:
    :param
    :return
    """
    print("---------- COMPUTE ----------")

    device = cp.cuda.Device(0)
    #print(device.use())
    thread_per_block = 1024
    num_blocks = 72
    print(f"Threads per block: {thread_per_block} #blocks: {num_blocks}")

    N = len(book)
    M = len(book[0][3])
    len_prices = len(list_prices)

    #debug = cp.zeros(M, dtype=cp.double)
    result = cp.zeros(N*len_prices*M).astype(cp.double)
    sizes = cp.array([N, M, len_prices, 0]).astype(cp.int32)

    GPU_func((N,),(thread_per_block,),
        (
        cp.array(list_prices, dtype=cp.double),
        cp.array([r[0]=='P' for r in book], dtype=cp.double),
        cp.array([r[1] for r in book], dtype=cp.double),
        cp.array([r[2] for r in book], dtype=cp.double),
        cp.array(np.array([r[3] for r in book]).flatten(), dtype=cp.double),
        cp.array([r[4] for r in book], dtype=cp.double),
        result,
        sizes))

    deltas_arr = result.reshape((N, len_prices, M)).sum(axis=0)
    return cp.asnumpy(deltas_arr)

def plot_2D(fx, fig_name):
    #plt.contourf(x,y,fx)
    plt.imshow(fx)
    plt.savefig(fig_name)

def test_findiff():
    # Define the grid
    x,y = np.linspace(-10,10, 200), np.linspace(-10,10, 200)
    X,Y = np.meshgrid(x, y)
    dx,dy = x[1]-x[0], y[1]-y[0]
    print((1/dx) * (1/dy))

    # Define function
    fx = np.sin(X**2+Y**2)
    plot_2D(fx, "fig_fx.png")
    print("Fx shape:", fx.shape)

    # === Laplacian with custom stencil ===
    offsets = [(0, 0), (1, 0), (-1, 0), (0, 1), (0, -1)]
    W = (1/dx)*(1/dy)
    stencil = findiff.stencils.Stencil(offsets, partials={(2, 0): W, (0, 2): W}, spacings=(1, 1))
    print(stencil)
    dfx = np.array(stencil(fx, on=[slice(1,-1), slice(1,-1)]))
    print("Stencil out shape: ", dfx.shape)
    plot_2D(dfx, "fig_dfx.png")
    print(dfx[1:5, 1:5], '\n')

    # === Laplacian with FinDiff ===
    d_dx = findiff.FinDiff(0, dx, 2) + findiff.FinDiff(1, dy, 2)
    print(d_dx.stencil(fx.shape).data[('C', 'C')])
    df_dx = d_dx(fx)
    plot_2D(df_dx, "fig_gt.png")
    print("Findiff out shape: ", df_dx.shape)
    print(df_dx[1:5, 1:5])

def laplacian_with_stencil(offset, fx, dx, dy):
    offsets = {
        "offset_default" : [(0, 0), (1, 0), (-1, 0), (0, 1), (0, -1)],
        "offset_cross" : [(0, 0), (-1, -1), (-1, 1), (1, -1), (1, 1)],
        "offset_Z" : [
            (-1, -1), (-1, 0), (-1,1), \
                    (0, 0),          \
            (1, -1),  (1, 0),   (1,1),
        ],
        "offset_full" : [
            (-1, -1), (-1, 0), (-1,1), \
            (0,-1),   (0, 0),  (0, 1),  \
            (1, -1),  (1, 0),   (1,1),
        ],
        "offset_top" : [(0, 0), (1,1),(-1,1)],
        "offset_top2" : [(0, 0), (1,1),(0, 1),(-1,1)],
        "offset_right" : [(0, 0), (1,1),(1,-1)],
        "offset_right2" : [(0, 0), (1,1),(1, 0),(1,-1)],
    }

    W = (1/dx)*(1/dy)
    stencil = findiff.stencils.Stencil(offsets[offset], partials={(2, 0): W, (0, 2): W}, spacings=(1, 1))
    dfx = np.array(stencil(fx, on=[slice(1,-1), slice(1,-1)]))
    return dfx

if __name__ == "__main__":
    #the goal of the script is to obtain a 2D array with the net delta of a portfolio/book of options, at different prices/times pairs.
    # book data structure goes like so:
    # book: array of contracts
    # ----- contract: data specific to the contract
    # ---------- call/put
    # ---------- strike price
    # ---------- IV
    # ---------- [dte] (days till expiration -- pre-calculated according to expiration for every time where delta needs to be calculated)
    # ---------- net position
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Do test")
    args = parser.parse_args()

    if args.test:
        test_findiff()
    else:
        array_prices = np.load(r"data/prices.npy", allow_pickle=True)
        prices = array_prices.tolist()

        full_book = np.load(r"data/book.npy", allow_pickle=True)
        full_book = full_book.tolist()

        # lets run code on sample for testing purposes
        sample_book = full_book[:100]
        print(f"Length sample book: {len(sample_book)}")
        compile(1, 1, 1)
        print("Compile done [Takes some time...]")
        #compile(len(sample_book), len(sample_book[0][3]), len(prices))

        start = time.time()
        deltas = compute_all(sample_book, prices)
        end = time.time()
        print(f"It took {end - start} seconds to run the computation")
        # sample data takes 30 seconds to run on my current setup

        expected_result = np.load(r"data/expected_output.npy")
        #print(deltas[0:4,:4], "\n\n", expected_result[:4,:4])
        print("Computation error: ", np.sum(deltas[:100]-expected_result))


        # === Findiff part : Compute laplacian ===

        # === With custom stencil ===
        dx, dy = 1, 1
        dfx = laplacian_with_stencil("offset_Z", expected_result, 1, 1)
        print("Custom offset:", dfx[1:5, 1:5], '\n')

        # === Default stencil ===
        d_dx = findiff.FinDiff(0, dx, 2) + findiff.FinDiff(1, dy, 2)
        df_dx = d_dx(expected_result, 1, 1)
        print("Default (FinDiff)", df_dx[1:5, 1:5], '\n')

        print("Diff from default:", np.mean(np.abs(dfx[1:-1, 1:-1]-df_dx[1:-1, 1:-1])))