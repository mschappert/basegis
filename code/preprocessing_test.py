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
# rpj_in = r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\preprocess_test"
# rpj_out = r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\preprocess_test\rpj_out" 
# rc_out = r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\preprocess_test\rc_out"

# def reproject_to_albers(input_raster, output_dir):
#     """
#     Reproject a raster to ESRI WKID 102033 (USA Contiguous Albers Equal Area Conic)
    
#     Args:
#         input_raster: Path to input raster
#         output_dir: Directory for output raster (if None, uses same directory as input)
        
#     Returns:
#         Path to output raster if successful, None otherwise
#     """
#     try:
#         # Set up output path
#         if output_dir is None:
#             output_dir = os.path.dirname(input_raster)
        
#         base_name = os.path.basename(input_raster)
#         output_name = f"{os.path.splitext(base_name)[0]}_P.tif"
#         output_raster = os.path.join(output_dir, output_name)
        
#         # Define the target projection (USA Contiguous Albers Equal Area Conic)
#         sr = arcpy.SpatialReference(102033)
        
#         # Perform reprojection
#         print(f"Reprojecting {base_name} to WKID 102033...")
#         temp_output = os.path.join(output_dir, f"temp_{os.path.splitext(base_name)[0]}.tif")
        
#         arcpy.management.ProjectRaster(
#             in_raster=input_raster,
#             out_raster=temp_output,
#             out_coor_system=sr,
#             resampling_type="BILINEAR",
#             cell_size="",
#             geographic_transform="",
#             in_coor_system=""
#         )
        
#         # Ensure output is GeoTIFF format
#         print(f"Converting to GeoTIFF format...")
#         arcpy.management.CopyRaster(
#             in_raster=temp_output,
#             out_rasterdataset=output_raster,
#             format="TIFF"
#         )
        
#         # Clean up temporary file
#         if os.path.exists(temp_output):
#             arcpy.management.Delete(temp_output)
        
#         print(f"Successfully reprojected: {output_raster}")
#         return output_raster
    
#     except Exception as e:
#         print(f"Error reprojecting {input_raster}: {str(e)}")
#         return None


# def reclassify_binary(input_raster, output_dir=None):
#     """
#     Reclassify raster values: 0 to 1, 1 to 2, and NoData to 1
    
#     Args:
#         input_raster: Path to input raster
#         output_dir: Directory for output raster (if None, uses same directory as input)
        
#     Returns:
#         Path to output raster if successful, None otherwise
#     """
#     try:
#         # Set up output path
#         if output_dir is None:
#             output_dir = os.path.dirname(input_raster)
        
#         base_name = os.path.basename(input_raster)
#         output_name = f"{os.path.splitext(base_name)[0]}_rc.tif"
#         output_raster = os.path.join(output_dir, output_name)
        
#         # Remove output file if it already exists
#         if arcpy.Exists(output_raster):
#             arcpy.management.Delete(output_raster)
#             print(f"Removed existing file: {output_raster}")
        
#         # Create a temporary geodatabase for scratch workspace
#         temp_gdb = os.path.join(arcpy.env.scratchFolder, "temp.gdb")
#         if not arcpy.Exists(temp_gdb):
#             arcpy.management.CreateFileGDB(arcpy.env.scratchFolder, "temp.gdb")
        
#         # Use scratch workspace for temporary files
#         with arcpy.EnvManager(scratchWorkspace=temp_gdb):
#             # Perform reclassification with specific remap string: 0->1, 1->2, NODATA->1
#             print(f"Reclassifying {base_name}...")
#             out_raster = arcpy.sa.Reclassify(
#                 in_raster=input_raster,
#                 reclass_field="Value",
#                 remap="0 1;1 2;NODATA 1",
#                 missing_values="DATA"
#             )
            
#             # Save as GeoTIFF
#             print(f"Saving as GeoTIFF: {output_raster}")
#             out_raster.save(output_raster)
        
#         print(f"Successfully reclassified: {output_raster}")
#         return output_raster
    
#     except Exception as e:
#         print(f"Error reclassifying {input_raster}: {str(e)}")
#         return None


# this works for using GWB for reclassifying the raster
#orig code from esri
# with arcpy.EnvManager(outputCoordinateSystem='PROJCS["South_America_Albers_Equal_Area_Conic",GEOGCS["GCS_South_American_1969",DATUM["D_South_American_1969",SPHEROID["GRS_1967_Truncated",6378160.0,298.25]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-60.0],PARAMETER["Standard_Parallel_1",-5.0],PARAMETER["Standard_Parallel_2",-42.0],PARAMETER["Latitude_Of_Origin",-32.0],UNIT["Meter",1.0]]', parallelProcessingFactor="80%", scratchWorkspace=r"Z:\scho\Eddie_Storage\Mikaya\Data\GWB_Preprosseing\Mosaic_p_rc"):
# out_raster = arcpy.sa.Reclassify(
# in_raster=r"Z:\scho\Eddie_Storage\Mikaya\Data\GWB_Preprosseing\Mosaic_P\mosaic_1995_P.tif",
# reclass_field="Value",
# remap="0 1;1 2;NODATA 1",
# missing_values="DATA"
# )
# out_raster.save(r"Z:\scho\Eddie_Storage\Mikaya\Data\GWB_Preprosseing\Mosaic_p_rc\mosaic_1995_P_rc.tif")

rc_in = r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\"
rc_out = r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\"

def reclassify_binary2(input_raster, output_dir=None):
    """
    Reclassify raster values with South America Albers projection: 0 to 1, 1 to 2, and NoData to 1
    
    Args:
        input_raster: Path to input raster
        output_dir: Directory for output raster (if None, uses same directory as input)
        
    Returns:
        Path to output raster if successful, None otherwise
    """
    try:
        # Set up output path
        if output_dir is None:
            output_dir = os.path.dirname(input_raster)
        
        base_name = os.path.basename(input_raster)
        output_name = f"{os.path.splitext(base_name)[0]}_rc.tif"
        output_raster = os.path.join(output_dir, output_name)
        
        # Remove output file if it already exists
        if arcpy.Exists(output_raster):
            arcpy.management.Delete(output_raster)
            print(f"Removed existing file: {output_raster}")
        
        # Use South America Albers projection and custom scratch workspace
        sa_albers = 'PROJCS["South_America_Albers_Equal_Area_Conic",GEOGCS["GCS_South_American_1969",DATUM["D_South_American_1969",SPHEROID["GRS_1967_Truncated",6378160.0,298.25]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-60.0],PARAMETER["Standard_Parallel_1",-5.0],PARAMETER["Standard_Parallel_2",-42.0],PARAMETER["Latitude_Of_Origin",-32.0],UNIT["Meter",1.0]]'
        
        print(f"Reclassifying {base_name} with South America Albers projection...")
        
        # Use EnvManager with South America Albers projection and parallel processing
        with arcpy.EnvManager(outputCoordinateSystem=sa_albers, 
                             parallelProcessingFactor="80%", 
                             scratchWorkspace=output_dir):
            
            out_raster = arcpy.sa.Reclassify(
                in_raster=input_raster,
                reclass_field="Value",
                remap="0 1;1 2;NODATA 1",
                missing_values="DATA"
            )
            
            # Save directly to output path
            out_raster.save(output_raster)
        
        print(f"Successfully reclassified: {output_raster}")
        return output_raster
    
    except Exception as e:
        print(f"Error reclassifying {input_raster}: {str(e)}")
        return None

    
if __name__ == "__main__":
    start_time = time.time()
 
    # Step 1: Reproject rasters to Albers
    # print("\n=== STEP 1: REPROJECTING RASTERS ===")
    # reprojected = process_rasters(
    #     reproject_to_albers, 
    #     rpj_in,
    #     use_multiprocessing=True,  # Use multiprocessing for reprojection
    #     output_dir=rpj_out
    # )
    
    ## Step 2: Reclassify the reprojected rasters
    print("\n=== STEP 2: RECLASSIFYING RASTERS ===")
    reclassed = process_rasters(
        reclassify_binary2, 
        rc_in,  # can be rpj_out 
        use_multiprocessing=True,  # Let ArcPy handle parallelism for reclassification
        output_dir=rc_out
    )
    
    # Report total execution time
    elapsed_time = time.time() - start_time
    minutes, seconds = divmod(elapsed_time, 60)
    print(f"\nTotal execution time: {int(minutes)} minutes {seconds:.2f} seconds")