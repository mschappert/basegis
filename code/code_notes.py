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
    