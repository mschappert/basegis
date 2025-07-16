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
arcpy.env.rasterStatistics = "NONE"

##########
# Folder Directories

# Clip
clip_in = r"D:\NEW_WORKING\MSPA_results_P"#r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_results"
clip_mask = r"D:\NEW_WORKING\clipping_raster_from_tiff_Albers\1990_P.tif"#r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_tiffs_to_use\1991_P_recoded.tif"
clip_out = r"D:\NEW_WORKING\MSPA_c"#




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

def process_rasters(process_func, input_dir, use_multiprocessing=True, **kwargs):
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


# def clip_rasters(input_raster, output_dir=clip_out, clip_mask=clip_mask):
#     try:
#         basename = os.path.basename(input_raster)
#         year = get_year(basename)
#         output_path = os.path.join(output_dir, f"{year}_c.tif")

#         # clip
#         if not arcpy.Exists(output_path):
#             arcpy.env.snapRaster = clip_mask
#             clipped = arcpy.sa.ExtractByMask(input_raster, clip_mask)
#             clipped.save(output_path)
            
#             # Force rebuild attribute table to only include existing values
#             arcpy.management.BuildRasterAttributeTable(output_path, "Overwrite")
            
#             print(f"Clip successful: {output_path}")
#         return output_path
#     except Exception as e:
#         print(f"Clip error: {str(e)}")
#         return None




# def clip_rasters(input_raster, output_dir=clip_out, clip_mask=clip_mask, reference_raster=None):
#     try:
#         basename = os.path.basename(input_raster)
#         year = get_year(basename)
#         output_path = os.path.join(output_dir, f"{year}_c.tif")

#         # clip
#         if not arcpy.Exists(output_path):
#             arcpy.env.snapRaster = clip_mask
#             clipped = arcpy.sa.ExtractByMask(input_raster, clip_mask)
#             clipped.save(output_path)
            
#             # Get unique values from reference if provided
#             if reference_raster:
#                 ref_values = [row[0] for row in arcpy.da.SearchCursor(reference_raster, "Value")]
            
#             # Rebuild target RAT
#             arcpy.management.BuildRasterAttributeTable(output_path, "Overwrite")
            
#             print(f"Clip successful: {output_path}")
#         return output_path
#     except Exception as e:
#         print(f"Clip error: {str(e)}")
#         return None



def clip_rasters(input_raster, output_dir=clip_out, clip_mask=clip_mask):
    try:
        basename = os.path.basename(input_raster)
        year = get_year(basename)
        output_path = os.path.join(output_dir, f"{year}_c.tif")

        if not arcpy.Exists(output_path):
            arcpy.env.snapRaster = clip_mask
            clipped = arcpy.sa.ExtractByMask(input_raster, clip_mask)
            
            # Define valid MSPA values
            valid_values = [0, 1, 3, 9, 17, 33, 35, 65, 67, 100, 101, 103, 105, 109, 117, 133, 135, 137, 165, 167, 169, 220]
            
            # Force raster to only contain valid values
            condition = arcpy.sa.InList(clipped, valid_values)
            cleaned = arcpy.sa.Con(condition, clipped, 0)
            cleaned.save(output_path)
            
            # Rebuild RAT
            arcpy.management.BuildRasterAttributeTable(output_path, "Overwrite")
            
            print(f"Clip successful: {output_path}")
        return output_path
    except Exception as e:
        print(f"Clip error: {str(e)}")
        return None


if __name__ == "__main__":
    print("Starting Processing")

    # ## Run clip stage
    print("Starting Clip")
    clip_start = time.time()
    clip_results = process_rasters(
        clip_rasters, 
        clip_in,
        use_multiprocessing=True,  # Set to True for multiprocessing.Pool
        output_dir=clip_out, 
        clip_mask=clip_mask
    )
    clip_duration = time.time() - clip_start
    print(f"Clip completed in {clip_duration:.2f} seconds")