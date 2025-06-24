# arcpy env location: C:\MyArcGISPro\bin\Python\envs\arcgispro-py3
import os
import arcpy
import multiprocessing

# ===== CONFIGURATION =====
arcpy.env.overwriteOutput = True
arcpy.env.parallelProcessingFactor = "75%"  # Use 75% of CPU cores
arcpy.CheckOutExtension("Spatial")

IN_FOLDER = r"C:\input_rasters"
CLIP_FEATURE = r"C:\clip_boundary.shp"
OUT_FOLDER = r"C:\output_results"
WINDOW_SIZE = 500 # radius
STAT = "SUM"  # SUM, MIN, MAX, MEAN, STD, etc.
NAME_CONVENTION = "mw_{basename}_{WINDOW_SIZE}"  # Customizable naming
CORES = 6  # Number of parallel processes
DIAMETER = 1000
RADIUS = DIAMETER / 2
UNITS = "MAP"

# Example: reclassify values 0-10 → 1, 10-20 → 2, 20-100 → 3
RC_TABLE = [
    [0, 10, 1],
    [10, 20, 2],
    [20, 100, 3]
]
RC_NODATA = "NODATA"

# ===== FUNCTION: CLIP RASTER =====
def clip_raster(input_raster, mask_raster):
    """Clip raster using raster mask and return clipped path"""
    try:
        # Create in-memory output
        clipped_path = f"in_memory/clipped_{os.path.basename(input_raster)}"
        arcpy.sa.ExtractByMask(input_raster, mask_raster).save(clipped_path)
        return clipped_path
    except Exception as e:
        raise RuntimeError(f"Clip failed for {input_raster}: {str(e)}")

# ===== FUNCTION: RECLASSIFY RASTER =====
def reclassify_raster(input_raster, RC_TABLE, RC_NODATA, OUT_FOLDER):
    try:
        reclass_path = f"in_memory/reclass_{os.path.basename(input_raster)}"
        remap = arcpy.sa.RemapRange(RC_TABLE)
        arcpy.sa.Reclassify(input_raster, "Value", remap, RC_NODATA).save(reclass_path)
        return reclass_path
    except Exception as e:
        raise RuntimeError(f"Reclass failed for {input_raster}: {str(e)}") 

# ===== FUNCTION: MOVING WINDOW =====
def apply_moving_window(input_raster, RADIUS, UNITS, OUT_FOLDER, STAT, DIAMETER):
    """Apply circular moving window and save result"""
    try:
        basename = os.path.splitext(os.path.basename(input_raster))[0]
        out_path = os.path.join(out_folder, f"focal_{basename}_{statistic.lower()}_{int(diameter)}m.tif")
        # Create neighborhood
        neighborhood = arcpy.sa.NbrCircle(RADIUS, UNITS)
        
        # Apply focal statistics
        focal_result = arcpy.sa.FocalStatistics(
            in_raster = input_raster,
            neighborhood = neighborhood,
            statistics_type = STAT
        )
        
        # Save output
        focal_result.save(output_path)
        return output_path
    except Exception as e:
        raise RuntimeError(f"Moving window failed for {input_raster}: {str(e)}")

# --- EXECUTION: STEP BY STEP ---
arcpy.env.workspace = IN_FOLDER
input_rasters = arcpy.ListRasters("*.tif")

# 1. Clip all rasters
os.makedirs(CLIPPED_FOLDER, exist_ok=True)
clipped_rasters = []
for ras in input_rasters:
    out = clip_raster(ras, CLIP_MASK_RASTER, CLIPPED_FOLDER)
    clipped_rasters.append(out)
print(f"Clipped {len(clipped_rasters)} rasters.")

# 2. Reclassify all clipped rasters
os.makedirs(RECLASS_FOLDER, exist_ok=True)
reclass_rasters = []
for ras in clipped_rasters:
    out = reclassify_raster(ras, RC_TABLE, RC_NODATA, RECLASS_FOLDER)
    reclass_rasters.append(out)
print(f"Reclassified {len(reclass_rasters)} rasters.")

# 3. Apply moving window to all reclassified rasters
os.makedirs(OUT_FOLDER, exist_ok=True)
output_rasters = []
for ras in reclass_rasters:
    out = apply_moving_window(ras, RADIUS, UNITS, STAT, OUT_FOLDER, DIAMETER)
    output_rasters.append(out)
print(f"Moving window applied to {len(output_rasters)} rasters.")







import arcpy
import os
import multiprocessing
from functools import partial

# --- CONFIGURATION ---
arcpy.env.overwriteOutput = True
arcpy.env.parallelProcessingFactor = "75%"
arcpy.CheckOutExtension("Spatial")

INPUT_FOLDER = r"C:\input_rasters"
CLIP_MASK_RASTER = r"C:\clip_mask.tif"
CLIPPED_FOLDER = r"C:\clipped_rasters"
RECLASS_FOLDER = r"C:\reclass_rasters"
OUTPUT_FOLDER = r"C:\output_results"
STATISTIC = "SUM"
WINDOW_DIAMETER = 1000
WINDOW_RADIUS = WINDOW_DIAMETER / 2
WINDOW_UNITS = "MAP"
RECLASS_TABLE = [
    [0, 10, 1],
    [10, 20, 2],
    [20, 100, 3]
]
RECLASS_NODATA = "NODATA"
CORES = 4  # Number of CPU cores to use

# --- FUNCTION: CLIP ---
def clip_raster(input_raster, mask_raster, out_folder):
    basename = os.path.splitext(os.path.basename(input_raster))[0]
    out_path = os.path.join(out_folder, f"clipped_{basename}.tif")
    arcpy.sa.ExtractByMask(input_raster, mask_raster).save(out_path)
    return out_path

# --- FUNCTION: RECLASSIFY ---
def reclassify_raster(input_raster, reclass_table, nodata_val, out_folder):
    basename = os.path.splitext(os.path.basename(input_raster))[0]
    out_path = os.path.join(out_folder, f"reclass_{basename}.tif")
    remap = arcpy.sa.RemapRange(reclass_table)
    arcpy.sa.Reclassify(input_raster, "Value", remap, nodata_val).save(out_path)
    return out_path

# --- FUNCTION: MOVING WINDOW ---
def apply_moving_window(input_raster, radius, units, statistic, out_folder, diameter):
    basename = os.path.splitext(os.path.basename(input_raster))[0]
    out_path = os.path.join(out_folder, f"focal_{basename}_{statistic.lower()}_{int(diameter)}m.tif")
    neighborhood = arcpy.sa.NbrCircle(radius, units)
    focal_result = arcpy.sa.FocalStatistics(
        in_raster=input_raster,
        neighborhood=neighborhood,
        statistics_type=statistic
    )
    focal_result.save(out_path)
    return out_path

# --- PARALLEL PROCESSING FUNCTIONS ---
def parallel_step(process_func, items, *args):
    """Run a processing step in parallel"""
    with multiprocessing.Pool(processes=CORES) as pool:
        # Create partial function with fixed arguments
        task_func = partial(process_func, *args)
        results = pool.map(task_func, items)
    return results

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    # Create output directories
    os.makedirs(CLIPPED_FOLDER, exist_ok=True)
    os.makedirs(RECLASS_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    # Get input rasters
    arcpy.env.workspace = INPUT_FOLDER
    input_rasters = arcpy.ListRasters("*.tif")
    
    # Step 1: Parallel clipping
    clip_args = (CLIP_MASK_RASTER, CLIPPED_FOLDER)
    clipped_rasters = parallel_step(clip_raster, input_rasters, *clip_args)
    print(f"Clipped {len(clipped_rasters)} rasters.")
    
    # Step 2: Parallel reclassification
    reclass_args = (RECLASS_TABLE, RECLASS_NODATA, RECLASS_FOLDER)
    reclass_rasters = parallel_step(reclassify_raster, clipped_rasters, *reclass_args)
    print(f"Reclassified {len(reclass_rasters)} rasters.")
    
    # Step 3: Parallel moving window
    mw_args = (WINDOW_RADIUS, WINDOW_UNITS, STATISTIC, OUTPUT_FOLDER, WINDOW_DIAMETER)
    output_rasters = parallel_step(apply_moving_window, reclass_rasters, *mw_args)
    print(f"Applied moving window to {len(output_rasters)} rasters.")
    
    print("All steps completed in parallel.")






import arcpy
import os
import multiprocessing
from functools import partial

# --- CONFIGURATION ---
arcpy.env.overwriteOutput = True
arcpy.env.parallelProcessingFactor = "75%"
arcpy.CheckOutExtension("Spatial")

INPUT_FOLDER = r"C:\input_rasters"
CLIP_MASK_RASTER = r"C:\clip_mask.tif"
RECLASS_FOLDER = r"C:\reclass_rasters"
OUTPUT_FOLDER = r"C:\output_results"
STATISTIC = "SUM"
WINDOW_DIAMETER = 1000
WINDOW_RADIUS = WINDOW_DIAMETER / 2
WINDOW_UNITS = "MAP"
RECLASS_TABLE = [
    [0, 10, 1],
    [10, 20, 2],
    [20, 100, 3]
]
RECLASS_NODATA = "NODATA"
CORES = 4  # Number of CPU cores to use

# --- FUNCTION: CLIP (IN-MEMORY) ---
def clip_raster_in_memory(input_raster, mask_raster):
    """Clip raster using raster mask, result stays in memory."""
    clipped = arcpy.sa.ExtractByMask(input_raster, mask_raster)
    return clipped

# --- FUNCTION: RECLASSIFY (TIFF OUTPUT) ---
def reclassify_raster_to_tif(input_raster, reclass_table, nodata_val, out_folder):
    """Reclassify raster and save as TIFF."""
    basename = os.path.splitext(os.path.basename(input_raster))[0]
    out_path = os.path.join(out_folder, f"reclass_{basename}.tif")
    remap = arcpy.sa.RemapRange(reclass_table)
    reclassed = arcpy.sa.Reclassify(input_raster, "Value", remap, nodata_val)
    reclassed.save(out_path)
    return out_path

# --- FUNCTION: MOVING WINDOW (TIFF OUTPUT) ---
def apply_moving_window_to_tif(input_raster, radius, units, statistic, out_folder, diameter):
    """Apply moving window and save as TIFF."""
    basename = os.path.splitext(os.path.basename(input_raster))[0]
    out_path = os.path.join(out_folder, f"focal_{basename}_{statistic.lower()}_{int(diameter)}m.tif")
    neighborhood = arcpy.sa.NbrCircle(radius, units)
    focal_result = arcpy.sa.FocalStatistics(
        in_raster=input_raster,
        neighborhood=neighborhood,
        statistics_type=statistic
    )
    focal_result.save(out_path)
    return out_path

# --- PARALLEL PROCESSING FUNCTIONS ---
def reclassify_worker(args):
    """Worker for parallel reclassification."""
    input_raster, mask_raster, reclass_table, nodata_val, out_folder = args
    # 1. Clip in memory
    clipped = clip_raster_in_memory(input_raster, mask_raster)
    # 2. Reclassify and save as TIFF
    basename = os.path.splitext(os.path.basename(input_raster))[0]
    out_path = os.path.join(out_folder, f"reclass_{basename}.tif")
    remap = arcpy.sa.RemapRange(reclass_table)
    reclassed = arcpy.sa.Reclassify(clipped, "Value", remap, nodata_val)
    reclassed.save(out_path)
    return out_path

def moving_window_worker(args):
    """Worker for parallel moving window."""
    input_raster, radius, units, statistic, out_folder, diameter = args
    # 1. Apply moving window and save as TIFF
    basename = os.path.splitext(os.path.basename(input_raster))[0]
    out_path = os.path.join(out_folder, f"focal_{basename}_{statistic.lower()}_{int(diameter)}m.tif")
    neighborhood = arcpy.sa.NbrCircle(radius, units)
    focal_result = arcpy.sa.FocalStatistics(
        in_raster=input_raster,
        neighborhood=neighborhood,
        statistics_type=statistic
    )
    focal_result.save(out_path)
    return out_path

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    # Create output directories
    os.makedirs(RECLASS_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    # Get input rasters
    arcpy.env.workspace = INPUT_FOLDER
    input_rasters = arcpy.ListRasters("*.tif")
    
    # Step 1: Parallel reclassification (includes in-memory clipping)
    reclass_args = [
        (ras, CLIP_MASK_RASTER, RECLASS_TABLE, RECLASS_NODATA, RECLASS_FOLDER)
        for ras in input_rasters
    ]
    with multiprocessing.Pool(processes=CORES) as pool:
        reclass_rasters = pool.map(reclassify_worker, reclass_args)
    print(f"Reclassified {len(reclass_rasters)} rasters (TIFFs).")
    
    # Step 2: Parallel moving window
    mw_args = [
        (ras, WINDOW_RADIUS, WINDOW_UNITS, STATISTIC, OUTPUT_FOLDER, WINDOW_DIAMETER)
        for ras in reclass_rasters
    ]
    with multiprocessing.Pool(processes=CORES) as pool:
        output_rasters = pool.map(moving_window_worker, mw_args)
    print(f"Applied moving window to {len(output_rasters)} rasters (TIFFs).")
    
    print("All steps completed in parallel.")

