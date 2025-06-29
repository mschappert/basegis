##########
# 06/27/ 2025
# This code was created to clip, reclassify, and run a moving window analysis on MPSA outputs
# This code is set up to run parallel processing to decrease processing time (especially for large files or a large batch of files)

# Clip: is needed due to how MSPA processed the data and added an extra boarder around the raster- this is not always the case
# Reclassify: is need to select specific landscape metrics to be turned into a binary image
# Moving Window: is needed to calculate a predefined statistic for a predefined spatial scale (landscape scale)

# Notes
# multiprocessing.Pool : is not needed since clip, reclassify and focal statistics support parallel processing factor
# remap : should in theory be the commented out code, however background values were not being properly set to 0 so Con was used
##########


import os
import arcpy
import multiprocessing
import re
import time

# ArcPy Configurations
arcpy.env.overwriteOutput = True
arcpy.env.parallelProcessingFactor = "200%"
arcpy.CheckOutExtension("Spatial")

# ArcPy Environments
# arcpy.env.snapRaster = "path/to/reference_raster.tif"
# arcpy.env.mask = "path/to/mask.tif"
# arcpy.env.extent = "xmin ymin xmax ymax"
# arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(26917)
# arcpy.env.compression = "LZW"
arcpy.env.pyramid = "NONE"
arcpy.env.rasterStatistics = "NONE"

# Clip parameters
clip_in = r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_results"
clip_mask = r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_tiffs_to_use\1991_P_recoded.tif"
clip_out = r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_c"

# Reclassification parameters
########### should set up remap values here instead of inside of the function - i was lazy 
#remap 1 =
# remap 2 = 
rc_in = r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_c"
edge_rc_out = r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_rc_edge"
area_rc_out = r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_rc_area"

# Moving window parameters
edge_mw_in = r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_rc_edge"
area_mw_in = r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_rc_area"
pn_mw_in = r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_rc_area"
mw_out = r"D:\Mikayla_RA\RA_S25\Time_Series\mw"

# Parameters that change
## RC
rc_type = "edge"  # "edge" or "area"
## MW
mw_type = "edge"  # "edge", "area", or "patchnum"
mw_radius = 500
stat = "SUM"  # variety

def get_year(filename):
    # Extract 4-digit year from filename using regex
    match = re.search(r"(\d{4})", filename)
    return match.group(1) if match else ""

def process_rasters(process_func, input_dir, **kwargs):
    """
    Process rasters in batch mode using parallel processing
    - process_func: Function to apply (clip_rasters, rc_rasters, or moving_window)
    - input_dir: Directory containing input rasters
    - kwargs: Function-specific parameters
    """
    arcpy.env.workspace = input_dir
    rasters = arcpy.ListRasters()
    print(f"Processing {len(rasters)}...")
    
    outputs = []
    for raster in rasters:
        input_path = os.path.join(input_dir, raster)
        result = process_func(input_path, **kwargs)
        outputs.append(result)
    success_count = sum(1 for p in outputs if p)
    print(f"Process complete: {success_count}/{len(rasters)} succeeded")
    return outputs
    
#########################################    

def clip_rasters (input_raster = clip_in, output_dir = clip_out, clip_mask = clip_mask):
    try:
        basename = os.path.basename(input_raster)
        year = get_year(basename)
        output_path = os.path.join(output_dir, f"{year}_c.tif")

        # clip
        if not arcpy.Exists(output_path):
            clipped = arcpy.sa.ExtractByMask(input_raster, clip_mask)
            clipped.save(output_path)
            print(f"Clip successful: {output_path}")
        return output_path
    except Exception as e:
        print(f"Clip error: {str(e)}")
        return None
   
def rc_rasters(input_raster = rc_in, type = rc_type):
    try:
        basename = os.path.basename(input_raster)
        year = get_year(basename)

        # remap based off type
        if type == "edge":
            output_path = os.path.join(edge_rc_out, f"{year}_rc_edge.tif")
            # remap = arcpy.sa.RemapValue([[3, 1],
            #                              [103, 1]])
            out_raster = arcpy.sa.Con((arcpy.sa.Raster(input_raster) == 3) | (arcpy.sa.Raster(input_raster) == 103), 1, 0)
        elif type == "area":
            output_path = os.path.join(edge_rc_out, f"{year}_rc_area.tif")
            # remap = arcpy.sa.RemapValue([[3, 1],
            #                              [103, 1],
            #                              [17, 1],
            #                              [117, 1]])
            out_raster = arcpy.sa.Con(
                (arcpy.sa.Raster(input_raster) == 3) | 
                (arcpy.sa.Raster(input_raster) == 103) | 
                (arcpy.sa.Raster(input_raster) == 17) | 
                (arcpy.sa.Raster(input_raster) == 117), 1, 0)
        else:
            print(f"Error: Undefined rc_type. Must be 'edge' or 'area'.")
            ################ if it doesn't fit in the if/else - set it to stop/ print: RC type not defined, skipping files        
        # reclassify
        if not arcpy.Exists(output_path):
            # reclass = arcpy.sa.Reclassify(input_raster, "VALUE", remap, "DATA") # setting to "DATA = sets all else to 0"
            # reclass.save(output_path)
            out_raster.save(output_path)
            print(f"Reclass successful: {output_path}")
        return output_path
    except Exception as e:
        print(f"Reclassify error: {str(e)}")
        return None
    
def moving_window(input_raster = None, output_dir = mw_out, type = mw_type, radius = mw_radius, stat = stat):
    try:
        basename = os.path.basename(input_raster)
        year = get_year(basename)
        output_path = os.path.join(output_dir, f"{year}_{type}_1km.tif")

        # mw
        if not arcpy.Exists(output_path):
            neighborhood = arcpy.sa.NbrCircle(radius, "MAP")
            focal = arcpy.sa.FocalStatistics(input_raster, neighborhood, stat)
            focal.save(output_path)
            print(f"Moving window successful: {output_path}")
        return output_path
        
    except Exception as e:
        print(f"Moving Window error: {str(e)}")
        return None 
    



# ==========
if __name__ == "__main__":
    print("Starting Processing")

    ## Run clip stage
    print("Starting Clip")
    clip_start = time.time()
    clip_results = process_rasters(
        clip_rasters, 
        clip_in, 
        output_dir = clip_out, 
        clip_mask=clip_mask
    )
    clip_duration = time.time() - clip_start
    print(f"Clip completed in {clip_duration:.2f} seconds")
    
    ## Run reclassification stage
    print("Staring Reclassification")
    rc_start = time.time()
    rc_results = process_rasters(
        rc_rasters, 
        rc_in, 
        type = rc_type
    )
    rc_duration = time.time() - rc_start
    print(f"Reclassification completed in {rc_duration:.2f} seconds")
    
    ## Run moving window stage
    print("Starting Moving Window")
    mw_start = time.time()
    mw_results = process_rasters(
        moving_window, 
        edge_mw_in, 
        output_dir = mw_out, 
        type = mw_type, 
        radius = mw_radius, 
        stat = stat
    )
    mw_duration = time.time() - mw_start
    print(f"Moving window completed in {mw_duration:.2f} seconds")

    ## Total processing time
    # total_time = time.time() - clip_start
    # print(f"\nTotal processing time: {total_time:.2f} seconds")