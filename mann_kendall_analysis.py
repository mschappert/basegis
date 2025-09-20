import numpy as np
import xarray as xr
import rioxarray
from scipy import stats
import stackstac

def mann_kendall_tau(data, axis=-1):
    """Calculate Mann-Kendall tau for time series data"""
    def mk_tau_1d(x):
        x = x[~np.isnan(x)]
        if len(x) < 2:
            return np.nan
        tau, _ = stats.kendalltau(np.arange(len(x)), x)
        return tau
    
    return np.apply_along_axis(mk_tau_1d, axis, data)

def stack_and_analyze(items, bbox=None, resolution=30):
    """Stack satellite images and calculate Mann-Kendall tau"""
    # Stack images using stackstac
    stack = stackstac.stack(
        items,
        bounds=bbox,
        resolution=resolution,
        dtype="float32"
    )
    
    # Calculate Mann-Kendall tau across time dimension
    tau = xr.apply_ufunc(
        mann_kendall_tau,
        stack,
        input_core_dims=[["time"]],
        output_core_dims=[[]],
        dask="allowed",
        vectorize=True
    )
    
    return stack, tau

# Example usage:
# items = your_stac_items  # STAC items from pystac-client
# bbox = [-120, 35, -119, 36]  # [minx, miny, maxx, maxy]
# stack, tau_result = stack_and_analyze(items, bbox)