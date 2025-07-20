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

# Reproject raster
# rpj_in = r"S:\Mikayla\DATA\Projects\AF\Time_Series\MSPA_mw_pn\rc"#r"B:\Mikayla\DATA\Projects\AF\Time_Series\MSPA_mw_area"
# rpj_out = r"S:\Mikayla\DATA\Projects\AF\Time_Series\MSPA_mw_pn\rc_P"#r"B:\Mikayla\DATA\Projects\AF\Time_Series\area_p"
# rpj_resampling = "BILINEAR" # Resampling type: "NEAREST", "BILINEAR", "CUBIC"
# # environment variables for reprojection
# snap_raster= r"S:\Mikayla\DATA\Projects\AF\Time_Series\MSPA_mw_area\1990_area_1km.tif"  # Snap raster for reprojection, set to None by default
# cell_size="31.8869969551851 31.8869969551851"
# # CRS - South America Albers Equal Area Conic
# target_crs='PROJCS["South_America_Albers_Equal_Area_Conic",GEOGCS["GCS_1990_P_b1",DATUM["D_South_American_1969",SPHEROID["GRS_1967",6378160.0,298.25]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-60.0],PARAMETER["Standard_Parallel_1",-5.0],PARAMETER["Standard_Parallel_2",-42.0],PARAMETER["Latitude_Of_Origin",-32.0],UNIT["Meter",1.0]]'
# rpj_ref = r"S:\Mikayla\DATA\Projects\AF\Time_Series\MSPA_mw_area\1990_area_1km.tif"  # Reference raster with target projection

# Batch Project parameters
reference_raster = r"B:\Mikayla\DATA\Projects\AF\NEW_WORKING\MSPA_results\1990_.tif"
rpj_in = r"B:\Mikayla\DATA\Projects\AF\5s_rerun\MSPA_results"  # Input directory with rasters to project
rpj_out = r"B:\Mikayla\DATA\Projects\AF\5s_rerun\MSPA_results_P"  # Output directory for projected rasters
rpj_resampling = "BILINEAR"  # Resampling method: "NEAREST", "BILINEAR", "CUBIC"

# Clip
clip_in = r"D:\NEW_WORKING\MSPA_results_P"#r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_results"
clip_mask = r"D:\NEW_WORKING\clipping_raster_from_tiff_Albers\1990_P.tif"#r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_tiffs_to_use\1991_P_recoded.tif"
clip_out = r"D:\NEW_WORKING\MSPA_c"#r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_c"

# Reclassification
rc_in = r"D:\NEW_WORKING\MSPA_results_P"#r"S:\Mikayla\DATA\Projects\AF\Time_Series\MSPA_c\MSPA_c" # r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_c"
edge_rc_out = r"D:\NEW_WORKING\MSPA_rc_edge" # r"S:\Mikayla\DATA\Projects\AF\Time_Series\MSPA_rc_edge" #r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_rc_edge"
area_rc_out = r"D:\NEW_WORKING\MSPA_rc_area" #r"S:\Mikayla\DATA\Projects\AF\Time_Series\MSPA_rc_area" #r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_rc_area"
rc_type = "area"  # "edge" or "area"sent
########### should set up remap values here instead of inside of the function - i was lazy 
#remap 1 =
# remap 2 = 

# RegionGroup
rg_out = r"D:\NEW_WORKING\rg"#r"S:\Mikayla\DATA\Projects\AF\Time_Series\rg" #r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_rg_patchnum"
neighbor="EIGHT"
grouping = "within"
link = "ADD_LINK"

## Reclass RG
rc_rg_in = r"D:\Mikayla_RA\RA_S25\NEW_WORKING\rg"#r"S:\Mikayla\DATA\Projects\AF\Time_Series\temp_rg_reclass"
rc_rg_out = r"D:\Mikayla_RA\RA_S25\NEW_WORKING\rg_rc" #r"S:\Mikayla\DATA\Projects\AF\Time_Series\temp_rc_rg_out"

# Moving window
edge_mw_in = r"D:\NEW_WORKING\MSPA_rc_edge" #r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_rc_edge"
area_mw_in = r"D:\NEW_WORKING\MSPA_rc_area"#r"S:\Mikayla\DATA\Projects\AF\Time_Series\MSPA_rc_area" # r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_rc_area"
pn_mw_in = r"D:\NEW_WORKING\rg_rc"#r"S:\Mikayla\DATA\Projects\AF\Time_Series\MSPA_rg_patchnum_1" #r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_rg_patchnum"
mw_out = r"D:\NEW_WORKING\mw_results"#r"S:\Mikayla\DATA\Projects\AF\Time_Series\mw_results" #r"M:\MW_RESULTS" #r"D:\Mikayla_RA\RA_S25\Time_Series\mw"
mw_type = "area"  # "edge", "area", or "pn"
mw_radius = 1000
stat = "SUM"  # VARIETY # SUM

# Reclass Region Group
rc_rg_in = r"S:\Mikayla\DATA\Projects\AF\Time_Series\temp_rc_rg_in"  # Input directory for reclass region group
rc_rg_out = r"S:\Mikayla\DATA\Projects\AF\Time_Series\temp_rc_rg_out2"  # Output directory for reclass region group


# TEST- TRY THIS OUT ###################################################

# masked rastets
# input (area mw)
raster_input = r"B:\Mikayla\DATA\Projects\AF\NEW_WORKING\MSPA_mw_area" #r"D:\NEW_WORKING\MSPA_mw_area" # need to change this for each type!!!!!!!!!!!!
# mask
mask = r"B:\Mikayla\DATA\Projects\AF\NEW_WORKING\binary_mask\binary_mask.tif"
# output
mw_masked_out = r"B:\Mikayla\DATA\Projects\AF\NEW_WORKING\MSPA_mw_masked" #r"D:\NEW_WORKING\MSPA_mw_masked" #r"S:\Mikayla\DATA\Projects\AF\Time_Series\MSPA_mw_masked" #r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_mw_masked"

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

def reproject_raster(input_raster, output_dir, reference_raster, resampling):
    """Project a single raster using a reference raster for coordinate system and cell size"""
    try:
        basename = os.path.basename(input_raster)
        year = get_year(basename)
        output_path = os.path.join(output_dir, f"{year}P.tif")
        
        if not arcpy.Exists(output_path):
            print(f"Projecting {basename}...")
            
            # Set environment variables
            arcpy.env.outputCoordinateSystem = arcpy.Describe(reference_raster).spatialReference
            arcpy.env.snapRaster = reference_raster
            arcpy.env.pyramid = "NONE"
            arcpy.env.extent = arcpy.Describe(reference_raster).extent
            arcpy.env.cellSize = reference_raster
            
            # Project raster
            arcpy.management.ProjectRaster(
                input_raster,
                output_path,
                arcpy.env.outputCoordinateSystem,
                resampling,
                arcpy.env.cellSize
            )
            print(f"Projection complete: {output_path}")
        return output_path
        
    except Exception as e:
        print(f"Reproject error: {str(e)}")
        return None

# orginal clip function
def clip_rasters(input_raster, output_dir=clip_out, clip_mask=clip_mask):
    try:
        basename = os.path.basename(input_raster)
        year = get_year(basename)
        output_path = os.path.join(output_dir, f"{year}_c.tif")

        # clip
        if not arcpy.Exists(output_path):
            arcpy.env.snapRaster = clip_mask # environment: snap raster
            clipped = arcpy.sa.ExtractByMask(input_raster, clip_mask) # clip
            clipped.save(output_path)
            print(f"Clip successful: {output_path}")
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
    
# clips out the weird values from the MSPA outputs on the edges
# turns all outside values to 0 
# this keeps it clean for terrset (which sometimes likes to add a boarder around the raster)
def mask_raster(input_raster, output_dir, mask_raster):
    """Apply binary mask - set values to 0 outside mask"""
    try:
        basename = os.path.basename(input_raster)
        output_path = os.path.join(output_dir, f"{basename}_mask.tif")
        
        if not arcpy.Exists(output_path):
            # Where mask = 1, keep original values; elsewhere set to 0
            masked = arcpy.sa.Con(mask_raster == 1, input_raster, 0)
            masked.save(output_path)
            print(f"Mask applied: {output_path}")
        return output_path
    except Exception as e:
        print(f"Mask application error: {str(e)}")
        return None

# ==========
if __name__ == "__main__":
    print("Starting Processing")

    # ## Reproject raster stage
    print("Starting Reprojection")
    rpj_start = time.time()
    rpj_results = process_rasters(
        reproject_raster,
        rpj_in,
        use_multiprocessing=True,
        output_dir=rpj_out,
        reference_raster=reference_raster,
        resampling=rpj_resampling
    )
    rpj_duration = time.time() - rpj_start
    print(f"Reprojection completed in {rpj_duration:.2f} seconds")
    
    # ## Run clip stage
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

    ## Apply mask to rasters
    print("Applying Mask to Rasters")
    mask_start = time.time()
    mask_results = process_rasters(
        mask_raster,
        raster_input,
        use_multiprocessing=True,
        output_dir=mw_masked_out,
        mask_raster= mask
        
    )
    mask_duration = time.time() - mask_start
    print(f"Masking completed in {mask_duration:.2f} seconds")

    # ## Total processing time
    # total_time = time.time() - clip_start
    # print(f"\nTotal processing time: {total_time:.2f} seconds")
    
    print("Processing complete!")