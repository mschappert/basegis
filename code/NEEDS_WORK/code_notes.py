# create mask
clip_mask = r"D:\NEW_WORKING\clipping_raster_from_tiff_Albers\1990_P.tif"#r"D:\Mikayla_RA\RA_S25\Time_Series\MSPA_tiffs_to_use\1991_P_recoded.tif"
mask_out = r"D:\NEW_WORKING\clipping_raster_from_tiff_Albers"


# create a binary mask to use for clipping the weird values from MSPA outputs on the edges
# def create_mask_raster(clip_mask, output_dir):
#     """Convert clip mask to binary mask (all values = 1)"""
#     try:
#         mask_path = os.path.join(output_dir, "binary_mask.tif")
#         if not arcpy.Exists(mask_path):
#             # Convert all non-zero values to 1
#             binary_mask = arcpy.sa.Con(arcpy.sa.IsNull(clip_mask), 0, 1)
#             binary_mask.save(mask_path)
#             print(f"Binary mask created: {mask_path}")
#         return mask_path
#     except Exception as e:
#         print(f"Mask creation error: {str(e)}")
#         return None

# with arcpy.EnvManager(outputCoordinateSystem='PROJCS["South_America_Albers_Equal_Area_Conic",GEOGCS["GCS_South_American_1969",DATUM["D_South_American_1969",SPHEROID["GRS_1967_Truncated",6378160.0,298.25]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-60.0],PARAMETER["Standard_Parallel_1",-5.0],PARAMETER["Standard_Parallel_2",-42.0],PARAMETER["Latitude_Of_Origin",-32.0],UNIT["Meter",1.0]]', cellSize="1990_P.tif", scratchWorkspace=r"D:\Mikaya\Data"):
#     out_raster = arcpy.sa.Con(
#         in_conditional_raster="1990_P.tif",
#         in_true_raster_or_constant=0,
#         in_false_raster_or_constant=1,
#         where_clause='IsNull("1990_P.tif.vat")'
#     )
#     out_raster.save(r"D:\Mikaya\Data\binary_mask.tif")

## Create binary mask
print("Creating Binary Mask")
binary_mask_path = create_mask_raster(clip_mask, mask_out)





# fixes discrepancies between 5 yr intervals and rest of years
# 5 yr - has classes for only values occuring 0-220
# rest of the years - has all vlaues from 0-220    
# worked- i just didnt like the data structure - think it was a problem with data not code
# def clip_rasters(input_raster, output_dir=clip_out, clip_mask=clip_mask):
#     try:
#         basename = os.path.basename(input_raster)
#         year = get_year(basename)
#         output_path = os.path.join(output_dir, f"{year}_c.tif")

#         if not arcpy.Exists(output_path):
#             arcpy.env.snapRaster = clip_mask
#             clipped = arcpy.sa.ExtractByMask(input_raster, clip_mask)
            
#             # Define valid MSPA values
#             valid_values = [0, 1, 3, 9, 17, 33, 35, 65, 67, 100, 101, 103, 105, 109, 117, 133, 135, 137, 165, 167, 169, 220]
            
#             # Force raster to only contain valid values
#             condition = arcpy.sa.InList(clipped, valid_values)
#             cleaned = arcpy.sa.Con(condition, clipped, 0)
#             cleaned.save(output_path)
            
#             # Rebuild RAT
#             arcpy.management.BuildRasterAttributeTable(output_path, "Overwrite")
            
#             print(f"Clip successful: {output_path}")
#         return output_path
#     except Exception as e:
#         print(f"Clip error: {str(e)}")
#         return None


# reprojection of raster from arc
arcpy.ImportToolbox(r"C:\Users\mksch\AppData\Local\Temp\ArcGISProTemp11152\Tmp.atbx")
with arcpy.EnvManager(outputCoordinateSystem='PROJCS["South_America_Albers_Equal_Area_Conic",GEOGCS["GCS_1990_P_b1",DATUM["D_South_American_1969",SPHEROID["GRS_1967",6378160.0,298.25]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-60.0],PARAMETER["Standard_Parallel_1",-5.0],PARAMETER["Standard_Parallel_2",-42.0],PARAMETER["Latitude_Of_Origin",-32.0],UNIT["Meter",1.0]]', resamplingMethod="BILINEAR", snapRaster="1990_.tif- to use", pyramid="NONE", extent='405828.0716645 31959.3459239858 2786734.47331731 3112402.6867796 PROJCS["South_America_Albers_Equal_Area_Conic",GEOGCS["GCS_1990_P_b1",DATUM["D_South_American_1969",SPHEROID["GRS_1967",6378160.0,298.25]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-60.0],PARAMETER["Standard_Parallel_1",-5.0],PARAMETER["Standard_Parallel_2",-42.0],PARAMETER["Latitude_Of_Origin",-32.0],UNIT["Meter",1.0]]', cellSize="1990_.tif- to use"):
    arcpy.Tmp.BatchProjectRaster1(
        in_raster="'1990_.tif- to use'",
        out_raster=r"B:\Mikayla\DATA\Projects\AF\5s_rerun\MSPA_results_P\%Name%P.tif",
        out_coor_system='PROJCS["South_America_Albers_Equal_Area_Conic",GEOGCS["GCS_1990_P_b1",DATUM["D_South_American_1969",SPHEROID["GRS_1967",6378160.0,298.25]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-60.0],PARAMETER["Standard_Parallel_1",-5.0],PARAMETER["Standard_Parallel_2",-42.0],PARAMETER["Latitude_Of_Origin",-32.0],UNIT["Meter",1.0]]',
        resampling_type="BILINEAR",
        cell_size="31.8869969551851 31.8869969551847",
        geographic_transform=None,
        Registration_Point=None,
        in_coor_system='PROJCS["South_America_Albers_Equal_Area_Conic",GEOGCS["GCS_1990_P_b1",DATUM["D_South_American_1969",SPHEROID["GRS_1967",6378160.0,298.25]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-60.0],PARAMETER["Standard_Parallel_1",-5.0],PARAMETER["Standard_Parallel_2",-42.0],PARAMETER["Latitude_Of_Origin",-32.0],UNIT["Meter",1.0]]',
        vertical="NO_VERTICAL"
    )

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


# def reproject_raster(input_raster, output_dir, reference_raster=rpj_ref, target_crs=target_crs, 
#                     cell_size=cell_size, snap_raster=snap_raster, resampling=rpj_resampling):
#     try:
#         basename = os.path.basename(input_raster)
#         output_path = os.path.join(output_dir, f"{os.path.splitext(basename)[0]}P.tif")
        
#         if not arcpy.Exists(output_path):
#             print(f"Reprojecting {basename}")
            
#             # Create spatial reference object
#             out_crs = arcpy.SpatialReference()
#             out_crs.loadFromString(target_crs)
            
#             arcpy.management.ProjectRaster(
#                 input_raster,
#                 output_path,
#                 out_crs,
#                 resampling_type=resampling,
#                 cell_size=cell_size
#             )
            
#             # Convert to preserve original data type
#             temp_raster = arcpy.sa.Int(output_path)
#             temp_raster.save(output_path)
#             print(f"Reproject successful: {output_path}")
#         return output_path
        
#     except Exception as e:
#         print(f"Reproject error: {str(e)}")
#         return None

    ## Reproject raster stage
    # print("Starting Reprojection")
    # rpj_start = time.time()
    # rpj_results = process_rasters(
    #     reproject_raster,
    #     rpj_in,
    #     use_multiprocessing=True,  # Set to True for multiprocessing.Pool
    #     output_dir=rpj_out,
    #     reference_raster=rpj_ref,  # Set to None if not using a reference raster
    #     target_crs=target_crs,  # Set to None if not using a target CRS
    #     cell_size=cell_size,  # Set to None if not using a specific cell size
    #     snap_raster=snap_raster,  # Set to None if not using
    #     resampling=rpj_resampling  # Resampling type: "NEAREST", "BILINEAR", "CUBIC"
    # )
    # rpj_duration = time.time() - rpj_start
    # print(f"Reprojection completed in {rpj_duration:.2f} seconds")
    
    
nodata_out = r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\nodata_test"  # Output directory for NoData conversion
def nodata_to_zero(input_raster, output_dir):
    """Converts all NoData values in a raster to 0 while preserving all other values."""
    try:
        basename = os.path.basename(input_raster)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        output_path = os.path.join(output_dir, f"{os.path.splitext(basename)[0]}.tif")
        
        if not arcpy.Exists(output_path):
            print(f"Converting NoData to 0 in {basename}")
            
            # Load input raster
            input_ras = arcpy.Raster(input_raster)
            
            # Simple approach: where IsNull, use 0; elsewhere keep original values
            result = arcpy.sa.Con(arcpy.sa.IsNull(input_ras), 0, input_ras)
            
            # Save with compression and no NoData value
            arcpy.env.compression = "LZW"
            arcpy.env.nodata = "NONE"
            result.save(output_path)
            print(f"Conversion complete: {output_path}")
        return output_path
    except Exception as e:
        print(f"Error converting NoData: {str(e)}")
        return None
    
## Convert NoData to zero
print("Converting NoData to Zero")
nodata_start = time.time()
nodata_results = process_rasters(
    nodata_to_zero,
    mw_masked_out,  # Change this to the directory with NoData values
    use_multiprocessing=True,
    output_dir=nodata_out
)




# clips out the weird values from the MSPA outputs on the edges
# turns all outside values to 0 
# this keeps it clean for terrset (which sometimes likes to add a boarder around the raster)
# def mask_raster(input_raster, output_dir, mask_raster):
#     """Apply binary mask - set values to 0 outside mask"""
#     try:
#         basename = os.path.basename(input_raster)
#         output_path = os.path.join(output_dir, f"{os.path.splitext(basename)[0]}_mask.tif")
        
#         if not arcpy.Exists(output_path):
#             # Where mask = 1, keep original values; elsewhere set to 0
#             masked = arcpy.sa.Con(mask_raster == 1, input_raster, 0)
#             masked.save(output_path)
#             print(f"Mask applied: {output_path}")
#         return output_path
#     except Exception as e:
#         print(f"Mask application error: {str(e)}")
#         return None
    
# def mask_raster(input_raster, output_dir, mask_raster):
#     """Apply binary mask - set values to 0 outside mask area."""
#     try:
#         # Setup output path
#         basename = os.path.basename(input_raster)
#         if not os.path.exists(output_dir):
#             os.makedirs(output_dir)
#         output_name = f"{os.path.splitext(basename)[0]}_mask.tif"
#         output_path = os.path.join(output_dir, output_name)

#         if not arcpy.Exists(output_path):
#             input_ras = arcpy.Raster(input_raster)
#             mask_ras = arcpy.Raster(mask_raster)

#             # Align the mask to input raster: use input raster as a snap raster
#             arcpy.env.snapRaster = input_ras
#             arcpy.env.extent = input_ras.extent
#             arcpy.env.cellSize = input_ras.meanCellWidth

#             # Fill NoData in mask with 0
#             filled_mask = arcpy.sa.Con(IsNull(mask_ras), 0, mask_ras)

#             # Apply mask: keep input raster values where mask == 1, set all others to 0
#             masked = arcpy.sa.Con(filled_mask == 1, input_ras, 0)
#             masked.save(output_path)
#             print(f"Mask applied: {output_path}")
#         return output_path

#     except Exception as e:
#         print(f"Mask application error: {str(e)}")
#         return None

# def mask_raster(input_raster, output_dir, mask_raster):
#     """Apply binary mask - set values to 0 outside mask area"""
#     try:
#         basename = os.path.basename(input_raster)
#         # Ensure output directory exists
#         if not os.path.exists(output_dir):
#             os.makedirs(output_dir)
        
#         output_name = f"{os.path.splitext(basename)[0]}_mask.tif"
#         output_path = os.path.join(output_dir, output_name)
        
#         if not arcpy.Exists(output_path):
#             print(f"Processing {basename}")
            
#             # Get input raster properties
#             input_ras = arcpy.Raster(input_raster)
#             mask_ras = arcpy.Raster(mask_raster)
            
#             # Set environment to match input raster
#             arcpy.env.extent = input_ras.extent
#             arcpy.env.cellSize = input_ras.meanCellWidth
#             arcpy.env.snapRaster = input_raster
            
#             # Create a constant raster of zeros with the same extent as input
#             zero_raster = arcpy.sa.CreateConstantRaster(0, "INTEGER", input_ras.meanCellWidth, input_ras.extent)
            
#             # Apply mask: where mask=1, use input values; elsewhere use zeros
#             # This preserves the exact extent of the input raster
#             masked = arcpy.sa.Con(mask_ras == 1, input_ras, zero_raster)
            
#             # Save with compression
#             arcpy.env.compression = "LZW"
#             masked.save(output_path)
#             print(f"Mask applied: {output_path}")
#         return output_path
#     except Exception as e:
#         print(f"Mask application error: {str(e)}")
#         return None
    
def clean_raster_edges(input_raster, output_dir, mask_raster):
    """
    Sets all values outside a specified area to 0 while maintaining the input raster's extent.
    Explicitly handles NoData values by converting them to 0.
    
    Parameters:
    - input_raster: Path to the input raster with residual values to clean
    - output_dir: Directory to save the cleaned raster
    - mask_raster: Binary raster (1=valid area, 0=outside) defining the area to keep
    """
    try:
        # Setup paths
        basename = os.path.basename(input_raster)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        output_path = os.path.join(output_dir, f"{os.path.splitext(basename)[0]}_masked.tif")
        
        if not arcpy.Exists(output_path):
            print(f"Cleaning {basename}")
            
            # Load rasters
            input_ras = arcpy.Raster(input_raster)
            mask_ras = arcpy.Raster(mask_raster)
            
            # Set environment to match input raster
            arcpy.env.extent = input_ras.extent
            arcpy.env.cellSize = input_ras.meanCellWidth
            
            # First, handle NoData in the input raster by converting to 0
            input_no_nulls = arcpy.sa.Con(arcpy.sa.IsNull(input_ras), 0, input_ras)
            
            # Then apply mask: where mask=1, keep input values; elsewhere set to 0
            # This preserves the exact extent of the input raster
            cleaned = arcpy.sa.Con(mask_ras == 1, input_no_nulls, 0)
            
            # Save with NoData value explicitly set to None (keeps all values including 0)
            arcpy.env.compression = "LZW"
            arcpy.env.nodata = "NONE"
            cleaned.save(output_path)
            print(f"Cleaned raster saved: {output_path}")
        return output_path
    except Exception as e:
        print(f"Error cleaning raster: {str(e)}")
        return None  

    ## Apply mask to rasters
    # print("Applying Mask to Rasters")
    # mask_start = time.time()
    # mask_results = process_rasters(
    #     clean_raster_edges, #mask_raster,
    #     raster_input,
    #     use_multiprocessing=True,
    #     output_dir=mw_masked_out,
    #     mask_raster= mask
        
    # )
    # mask_duration = time.time() - mask_start
    # print(f"Masking completed in {mask_duration:.2f} seconds")
    
    
    
    
    
    
    
clean_in = r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\clean_in"
clean_out = r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\clean_out"
mask = r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\binary_mask\binary_mask.tif"  # Binary mask raster to define valid area


def clean_raster_edges(input_raster, output_dir, mask_raster):
    """
    Cleans residual values around raster edges using a mask.
    
    Parameters:
    - input_raster: Path to the input raster with residual values
    - output_dir: Directory to save the cleaned raster
    - mask_raster: Binary raster (1=valid area, 0=outside) defining the area to keep
    """
    try:
        basename = os.path.basename(input_raster)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        output_path = os.path.join(output_dir, f"{os.path.splitext(basename)[0]}_clean.tif")
        
        if not arcpy.Exists(output_path):
            print(f"Cleaning {basename}")
            
            # First, extract by mask to get only the valid area
            extracted = arcpy.sa.ExtractByMask(input_raster, mask_raster)
            
            # Then, create a constant raster of zeros with the same extent as input
            input_ras = arcpy.Raster(input_raster)
            zero_raster = arcpy.sa.CreateConstantRaster(0, "INTEGER", input_ras.meanCellWidth, input_ras.extent)
            
            # Mosaic the extracted raster on top of the zero raster
            # This ensures zeros everywhere outside the mask
            arcpy.management.MosaicToNewRaster(
                [zero_raster, extracted], 
                os.path.dirname(output_path),
                os.path.basename(output_path),
                pixel_type="32_BIT_UNSIGNED",
                number_of_bands=1,
                mosaic_method="LAST",
                mosaic_colormap_mode="FIRST"
            )
            
            print(f"Cleaned raster saved: {output_path}")
        return output_path
    except Exception as e:
        print(f"Error cleaning raster: {str(e)}")
        return None
    
        # In your main script
    print("Cleaning Raster Edges")
    clean_start = time.time()
    clean_results = process_rasters(
        clean_raster_edges,
        clean_in, # input raster
        use_multiprocessing=True,
        output_dir=clean_out,
        mask_raster=mask
    )
    clean_duration = time.time() - clean_start
    print(f"Cleaning completed in {clean_duration:.2f} seconds")
    
    
    
    
    
    # from rst converion .py file
import os
import glob
from multiprocessing import Pool, cpu_count
from osgeo import gdal

# Configure GDAL error handling
gdal.UseExceptions()  # Enable exceptions for better error handling

def convert_tiff_to_rst(input_file):
    """Convert a single TIFF file to RST format"""
    try:
        output_file = input_file.replace('.tif', '.rst').replace('.tiff', '.rst')
        
        # Open source dataset with proper access mode
        src_ds = gdal.Open(input_file, gdal.GA_ReadOnly)
        if src_ds is None:
            print(f"Failed to open {input_file}")
            return False
            
        # Get driver and verify it's available
        driver = gdal.GetDriverByName('RST')
        if driver is None:
            print(f"RST driver not available in this GDAL installation")
            return False
            
        # Create RST file with creation options
        dst_ds = driver.CreateCopy(output_file, src_ds, 0)  # 0 = don't strict copy
        
        # Ensure proper cleanup to prevent file locks
        dst_ds.FlushCache()  # Flush pending writes
        
        # Clean up
        src_ds = None
        dst_ds = None
        
        print(f"Converted: {os.path.basename(input_file)} -> {os.path.basename(output_file)}")
        return True
        
    except Exception as e:
        print(f"Error converting {input_file}: {str(e)}")
        return False

def batch_convert_tiff_to_rst(input_folder, num_processes=None):
    """Batch convert all TIFF files in a folder to RST format"""
    
    # Configure GDAL for optimal performance
    gdal.SetConfigOption('GDAL_CACHEMAX', '512')  # 512MB cache
    
    # Find all TIFF files in one pass with more efficient pattern
    tiff_files = []
    for ext in ['.tif', '.tiff']:
        tiff_files.extend(glob.glob(os.path.join(input_folder, f'**/*{ext}'), recursive=True))
    
    if not tiff_files:
        print("No TIFF files found in the specified folder")
        return
    
    print(f"Found {len(tiff_files)} TIFF files to convert")
    
    # Use optimal number of processes (GDAL operations are I/O bound)
    if num_processes is None:
        # For GDAL operations, using too many processes can cause I/O bottlenecks
        num_processes = min(cpu_count(), 4)  # Limit to reasonable number
    
    print(f"Using {num_processes} processes for conversion")
    
    # Process files in parallel
    with Pool(processes=num_processes) as pool:
        results = pool.map(convert_tiff_to_rst, tiff_files)
    
    successful = sum(results)
    print(f"\nConversion complete: {successful}/{len(tiff_files)} files converted successfully")

if __name__ == "__main__":
    # Register all GDAL drivers at startup
    gdal.AllRegister()
    
    # Check if RST driver is available
    if gdal.GetDriverByName('RST') is None:
        print("ERROR: RST driver not available in this GDAL installation")
        print("Please ensure GDAL is properly configured with RST support")
        exit(1)
        
    # Set your input folder path here
    input_folder = input("Enter the folder path containing TIFF files: ").strip()
    
    if not os.path.exists(input_folder):
        print("Folder does not exist!")
    else:
        try:
            processes = input("Number of processes to use (press Enter for auto): ").strip()
            processes = int(processes) if processes else None
        except ValueError:
            processes = None
            
        batch_convert_tiff_to_rst(input_folder, processes)