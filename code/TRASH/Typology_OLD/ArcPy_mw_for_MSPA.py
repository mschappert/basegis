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
arcpy.env.parallelProcessingFactor = "100%"  # Use percentage format for built-in parallel processing
arcpy.CheckOutExtension("Spatial")
cores = multiprocessing.cpu_count() # Use all cores #- 8 can adjust how many cores are left out

# ArcPy Environments
# arcpy.env.snapRaster = "path/to/reference_raster.tif"
# arcpy.env.mask = "path/to/mask.tif"
# arcpy.env.extent = "xmin ymin xmax ymax"
# arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(26917)
# arcpy.env.compression = "LZW"
arcpy.env.pyramid = "NONE"
#arcpy.env.rasterStatistics = "NONE"

##########
# Parameters

# Clip - This step is not necessary - only if needed 
clip_in = r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\clip_in" # Input directory with rasters to clip
clip_mask = r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\binary_mask\binary_mask_shrink40.tif" # Mask raster for clipping
clip_out = r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\clip_out"       

# Reclassification
rc_in = r"D:\NEW_WORKING\MSPA_results_P" # Input directory with rasters to reclassify
edge_rc_out = r"D:\NEW_WORKING\MSPA_rc_edge" # Output directory for edge reclassification
area_rc_out = r"D:\NEW_WORKING\MSPA_rc_area" # Output directory for area reclassification
rc_type = "area"  # Select type: "edge" or "area"
# Note: patch number is not reclassified, since it is derived from area using RegionGroup

# RegionGroup- only to be run on area to calculate patch number
rg_out = r"D:\NEW_WORKING\rg" # Output directory for region group results
neighbor="EIGHT" # Specifies neighbor connectivity: "FOUR" or "EIGHT"
grouping = "within" # Assigns a zone for each group of connected cells
link = "ADD_LINK" # Assings an ID to each group of connected cells (each group has a unique ID)
# Note: Input is area_rc_out by default, which is how the patch number is derived from area

# Reclass Region Group- only to be used to set background values to 0
rc_rg_in = r"D:\Mikayla_RA\RA_S25\NEW_WORKING\rg" # Input directory for reclass region group
rc_rg_out = r"D:\Mikayla_RA\RA_S25\NEW_WORKING\rg_rc" # Output directory for reclass region group

# Moving window
edge_mw_in = r"D:\NEW_WORKING\MSPA_rc_edge" # Input folder for data that will be used for edge moving window
area_mw_in = r"D:\NEW_WORKING\MSPA_rc_area" # Input folder for data that will be used for area moving window
pn_mw_in = r"D:\NEW_WORKING\rg_rc" # Input folder for data that will be used for patch number moving window
mw_out = r"D:\NEW_WORKING\mw_results" # Output folder to hold moving window results
mw_type = "area"  # Select type: either "edge", "area", or "pn"
mw_radius = 1000
stat = "SUM"  # Select statistics type: "VARIETY", or "SUM"

#########################################    

def init_worker():
    """Initialize ArcPy environment for multiprocessing workers"""
    import arcpy
    arcpy.env.overwriteOutput = True
    arcpy.env.parallelProcessingFactor = "0"  # Disable parallel processing in workers
    arcpy.CheckOutExtension("Spatial")
    arcpy.env.pyramid = "NONE"
    arcpy.env.rasterStatistics = "NONE"

def get_year(filename):
    # Extract 4-digit year from filename using regex
    match = re.search(r"(\d{4})", filename)
    return match.group(1) if match else ""

def process_rasters(process_func, input_dir, use_multiprocessing=False, **kwargs):
    """
    Process rasters in batch mode using parallel processing
    - process_func: Function to apply (clip_rasters, rc_rasters, or moving_window)
    - input_dir: Directory containing input rasters
    - use_multiprocessing: 
        * True = Batch parallelism (multiple files at once) - for tools WITHOUT native parallel support
        * False = Tool parallelism (ArcPy handles it) - for tools WITH native parallel support
    - kwargs: Function-specific parameters
    """
    arcpy.env.workspace = input_dir
    rasters = arcpy.ListRasters()
    
    if not rasters:
        print(f"No rasters found in directory: {input_dir}")
        return []
    
    print(f"Processing {len(rasters)} rasters...")

    if use_multiprocessing:
        # BATCH PARALLELISM: Process multiple files simultaneously
        # Use when ArcPy tool doesn't have native parallel processing
        arcpy.env.parallelProcessingFactor = "0"  # Disable tool-level parallelism
        input_paths = [os.path.join(input_dir, r) for r in rasters]
        func = partial(process_func, **kwargs)
        
        print(f"Using {cores} processes for multiprocessing")
        with multiprocessing.Pool(processes=cores, initializer=init_worker) as pool:
            outputs = pool.map(func, input_paths)
    else:
        # TOOL PARALLELISM: Let ArcPy tool handle parallelism internally
        # Use when ArcPy tool has native parallel processing support
        arcpy.env.parallelProcessingFactor = "100%"  # Enable tool-level parallelism
        outputs = []
        for raster in rasters:
            input_path = os.path.join(input_dir, raster)
            result = process_func(input_path, **kwargs)
            outputs.append(result)
    
    success_count = sum(1 for p in outputs if p)
    print(f"Process complete: {success_count}/{len(rasters)} succeeded")
    return outputs
    
#########################################    

# clip function with NoData set to 0
def clip_rasters(input_raster, output_dir=clip_out, clip_mask=clip_mask):
    try:
        basename = os.path.basename(input_raster)
        year = get_year(basename)
        output_path = os.path.join(output_dir, f"{year}_c.tif")

        # clip
        if not arcpy.Exists(output_path):
            arcpy.env.snapRaster = clip_mask # environment: snap raster
            
            # First extract by mask
            clipped = arcpy.sa.ExtractByMask(input_raster, clip_mask)
            
            # Then convert NoData to 0
            with arcpy.EnvManager(nodata="NONE"):
                final_raster = arcpy.sa.Con(arcpy.sa.IsNull(clipped), 0, clipped)
                final_raster.save(output_path)
                
            print(f"Clip successful with NoData set to 0: {output_path}")
        return output_path
    except Exception as e:
        print(f"Clip error: {str(e)}")
        return None
    
def rc_rasters(input_raster, rc_type=rc_type):
    try:
        basename = os.path.basename(input_raster)
        year = get_year(basename)

        # remap based off type
        if rc_type == "edge":
            output_path = os.path.join(edge_rc_out, f"{year}_rc_edge.tif")
            # in reality- this should work but it doesn't - so i used Con
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
            return None
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
        
def region_group(input_raster, output_dir=rg_out, number_neighbors=neighbor, zone_connectivity=grouping, add_link=link, excluded_value=0):
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
    
# sometimes has weird background values that need to be set to 0
def rc_rg_rasters(input_raster, output_dir=rc_rg_out):
    try:
        basename = os.path.basename(input_raster)
        output_path = os.path.join(output_dir, f"{os.path.splitext(basename)[0]}_rc.tif")
        
        if not arcpy.Exists(output_path):
            out_raster = arcpy.sa.Con(arcpy.sa.Raster(input_raster) == 1, 0, arcpy.sa.Raster(input_raster))
            out_raster.save(output_path)
            print(f"Reclass successful: {output_path}")
        return output_path
    except Exception as e:
        print(f"Reclass error: {str(e)}")
        return None

def moving_window(input_raster, output_dir=mw_out, type=mw_type, radius=mw_radius, stat=stat):
    try:
        basename = os.path.basename(input_raster)
        year = get_year(basename)
        output_path = os.path.join(output_dir, f"{year}_{type}_1km.tif")

        # mw
        if not arcpy.Exists(output_path):
            print(f"Processing {basename}")
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

    # ## Run clip stage (if needed)
    # print("Starting Clip")
    # clip_start = time.time()
    # clip_results = process_rasters(
    #     clip_rasters, 
    #     clip_in,
    #     use_multiprocessing=True,  # Set to True for multiprocessing.Pool
    #     output_dir=clip_out, 
    #     clip_mask=clip_mask
    # )
    # clip_duration = time.time() - clip_start
    # print(f"Clip completed in {clip_duration:.2f} seconds")
    
    # ## Run reclassification stage
    # print("Starting Reclassification")
    # rc_start = time.time()
    # rc_results = process_rasters(
    #     rc_rasters, 
    #     rc_in,
    #     use_multiprocessing=True,  # Set to True for multiprocessing.Pool
    #     rc_type=rc_type
    # )
    # rc_duration = time.time() - rc_start
    # print(f"Reclassification completed in {rc_duration:.2f} seconds")
    
    # ## Region Group - for patchnumber only 
    # print("Starting RegionGroup")
    # rg_start = time.time()
    # rg_results = process_rasters(
    #     region_group,
    #     area_rc_out,
    #     use_multiprocessing=True,  # Set to True for multiprocessing.Pool
    #     output_dir=rg_out,
    #     number_neighbors=neighbor,
    #     zone_connectivity=grouping,
    #     add_link=link,
    #     excluded_value=0
    # )
    # rg_duration = time.time() - rg_start
    # print(f"RegionGroup completed in {rg_duration:.2f} seconds")
    
    ## Reclass region group raster to fix background values
    # print("Starting Reclass Region Group")
    # rc_rg_start = time.time()
    # rc_rg_results = process_rasters(
    #     rc_rg_rasters,
    #     rc_rg_in,
    #     use_multiprocessing=True,  # Set to True for multiprocessing.Pool
    #     output_dir=rc_rg_out
    # )
    # rc_rg_duration = time.time() - rc_rg_start
    # print(f"Reclass Region Group completed in {rc_rg_duration:.2f} seconds")

    ## Run moving window stage
    # print("Starting Moving Window")
    # mw_start = time.time()
    # mw_results = process_rasters(
    #     moving_window, 
    #     area_mw_in, # change with mw type 
    #     use_multiprocessing=True,  # Set to True for multiprocessing.Pool
    #     output_dir=mw_out, 
    #     type=mw_type, 
    #     radius=mw_radius, 
    #     stat=stat
    # )
    # mw_duration = time.time() - mw_start
    # print(f"Moving window completed in {mw_duration:.2f} seconds")
    
    print("Processing complete!")