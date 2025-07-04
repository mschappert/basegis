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
# location for arcpy environment: C:\MyArcGISPro\bin\Python\envs\arcgispro-py3\python.exe
# location for arcpy idle: C:\MyArcGISPro\bin\Python\envs\arcgispro-py3\Lib\idlelib\idle.bat

# Tools used
# Extract by Mask (Raster Clip): https://pro.arcgis.com/en/pro-app/latest/tool-reference/spatial-analyst/extract-by-mask.htm
# Remap: https://pro.arcgis.com/en/pro-app/latest/arcpy/spatial-analyst/remap.htm
# Con: https://pro.arcgis.com/en/pro-app/latest/tool-reference/spatial-analyst/extract-by-mask.htm
# RegionGroup: https://pro.arcgis.com/en/pro-app/latest/tool-reference/spatial-analyst/region-group.htms
# Focal Statistics: https://pro.arcgis.com/en/pro-app/latest/tool-reference/spatial-analyst/focal-statistics.htm
##########
import os
import arcpy
import multiprocessing
import re
import time
import sys
from functools import partial

##########
# ArcPy Configurations
arcpy.env.overwriteOutput = True
arcpy.env.parallelProcessingFactor = "200" #"0" # "200%"- use 0 if using processing pool
arcpy.CheckOutExtension("Spatial")
cores = multiprocessing.cpu_count() - 1  # Leave 1 core free

# ArcPy Environments
# arcpy.env.snapRaster = "path/to/reference_raster.tif"
# arcpy.env.mask = "path/to/mask.tif"
# arcpy.env.extent = "xmin ymin xmax ymax"
# arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(26917)
# arcpy.env.compression = "LZW"
arcpy.env.pyramid = "NONE"
arcpy.env.rasterStatistics = "NONE"

##########
# Folder Directories
# Clip
clip_in = r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_results"
clip_mask = r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_tiffs_to_use\1991_P_recoded.tif"
clip_out = r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_c"
# Reclassification
rc_in = r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_c"
edge_rc_out = r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_rc_edge"
area_rc_out = r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_rc_area"
# RegionGroup
rg_out = "D:\Mikayla_RA\RA_S25\Time_Series\MSPA_rg_patchnum"
# Moving window
edge_mw_in = r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_rc_edge"
area_mw_in = r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_rc_area"
pn_mw_in = r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_rc_area"
mw_out = r"D:\Mikayla_RA\RA_S25\Time_Series\mw"

# Parameters that change
## RC
rc_type = "area"  # "edge" or "area"
########### should set up remap values here instead of inside of the function - i was lazy 
#remap 1 =
# remap 2 = 
## Region Group
neighbor="EIGHT"
grouping = "within"
link = "ADD_LINK"
## MW
mw_type = "pn"  # "edge", "area", or "patchnum"
mw_radius = 500
stat = "VARIETY"  # variety

#########################################    

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

    # original
    # with multiprocessing.Pool(processes=cores) as pool:
    #     results = []
    # for raster in rasters:
    #     input_path = os.path.join(input_dir, raster)
    #     # Pass arguments as keyword arguments
    #     results.append(pool.apply_async(process_func, (input_path,), kwargs))
    # outputs = [r.get() for r in results]
    
    ## for using built in parallel processing
    outputs = []
    for raster in rasters:
        input_path = os.path.join(input_dir, raster)
        result = process_func(input_path, **kwargs)
        outputs.append(result)
    success_count = sum(1 for p in outputs if p)

    # for using processing pool instead of built in parallel processing
    # input_paths = [os.path.join(input_dir, r) for r in rasters]
    # # Use partial to fix constant kwargs for process_func
    # func = partial(process_func, **kwargs)
    # with multiprocessing.Pool(processes=cores) as pool:
    #     outputs = pool.map(func, input_paths)
    # success_count = sum(1 for p in outputs if p)

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
   
def rc_rasters(input_raster = rc_in, rc_type = rc_type):
    try:
        basename = os.path.basename(input_raster)
        year = get_year(basename)

        # remap based off type
        if rc_type == "edge":
            output_path = os.path.join(edge_rc_out, f"{year}_rc_edge.tif")
            # remap = arcpy.sa.RemapValue([[3, 1],
            #                              [103, 1]])
            out_raster = arcpy.sa.Con((arcpy.sa.Raster(input_raster) == 3) | (arcpy.sa.Raster(input_raster) == 103), 1, 0)
        elif rc_type == "area":
            output_path = os.path.join(area_rc_out, f"{year}_rc_area.tif")
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
            # to be used when using RemapValue
            # reclass = arcpy.sa.Reclassify(input_raster, "VALUE", remap, "DATA") # setting to "DATA = sets all else to 0"
            # reclass.save(output_path)
            out_raster.save(output_path)
            print(f"Reclass successful: {output_path}")
        return output_path
    except Exception as e:
        print(f"Reclassify error: {str(e)}")
        return None
    
def region_group(input_raster = area_rc_out, output_dir = rg_out, number_neighbors = neighbor, zone_connectivity = grouping, add_link = link, excluded_value = 0):
    try:
        basename = os.path.basename(input_raster)
        year = get_year(basename)
        output_path = os.path.join(output_dir, f"{year}_area_rg.tif")

        # region group
        if not arcpy.Exists(output_path):
            rg = arcpy.sa.RegionGroup(
                input_raster,
                number_neighbors,
                zone_connectivity,
                add_link,
                excluded_value
            )
            rg.save(output_path)
            print(f"RegionGroup successful: {output_path}")
        return output_path
    except Exception as e:
        print(f"Clip error: {str(e)}")
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

    # ## Run clip stage
    # print("Starting Clip")
    # clip_start = time.time()
    # clip_results = process_rasters(
    #     clip_rasters, 
    #     clip_in, 
    #     output_dir = clip_out, 
    #     clip_mask=clip_mask
    # )
    # clip_duration = time.time() - clip_start
    # print(f"Clip completed in {clip_duration:.2f} seconds")
    
    # ## Run reclassification stage
    # print("Staring Reclassification")
    # rc_start = time.time()
    # rc_results = process_rasters(
    #     rc_rasters, 
    #     rc_in, 
    #     rc_type = rc_type
    # )
    # rc_duration = time.time() - rc_start
    # print(f"Reclassification completed in {rc_duration:.2f} seconds")
    
    ## Region Group - for patchnumber only 
    print("Starting RegionGroup")
    rg_start = time.time()
    rg_results = process_rasters(
        region_group,
        area_rc_out,
        output_dir = rg_out,
        number_neighbors = neighbor,
        zone_connectivity = grouping,
        add_link = link,
        excluded_value = 0

    )
    rg_duration = time.time() - rg_start
    print(f"RegionGroup completed in {rg_duration:.2f} seconds")

    ## Run moving window stage
    # print("Starting Moving Window")
    # mw_start = time.time()
    # mw_results = process_rasters(
    #     moving_window, 
    #     area_mw_in,         # change this as needed
    #     output_dir = mw_out, 
    #     type = mw_type, 
    #     radius = mw_radius, 
    #     stat = stat
    # )
    # mw_duration = time.time() - mw_start
    # print(f"Moving window completed in {mw_duration:.2f} seconds")

    # ## Total processing time
    # total_time = time.time() - clip_start
    # print(f"\nTotal processing time: {total_time:.2f} seconds")