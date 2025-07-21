# reprojects data to match the binary mask
# with arcpy.EnvManager(outputCoordinateSystem='PROJCS["South_America_Albers_Equal_Area_Conic",GEOGCS["GCS_South_American_1969",DATUM["D_South_American_1969",SPHEROID["GRS_1967_Truncated",6378160.0,298.25]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-60.0],PARAMETER["Standard_Parallel_1",-5.0],PARAMETER["Standard_Parallel_2",-42.0],PARAMETER["Latitude_Of_Origin",-32.0],UNIT["Meter",1.0]]', resamplingMethod="BILINEAR", pyramid="NONE", cellSize="1992_area_1km.tif"):
#     arcpy.management.ProjectRaster(
#         in_raster="1992_area_1km.tif",
#         out_raster=r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\masked_test\1992_area_1km_p.tif",
#         out_coor_system='PROJCS["South_America_Albers_Equal_Area_Conic",GEOGCS["GCS_South_American_1969",DATUM["D_South_American_1969",SPHEROID["GRS_1967_Truncated",6378160.0,298.25]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-60.0],PARAMETER["Standard_Parallel_1",-5.0],PARAMETER["Standard_Parallel_2",-42.0],PARAMETER["Latitude_Of_Origin",-32.0],UNIT["Meter",1.0]]',
#         resampling_type="BILINEAR",
#         cell_size="31.8869969551851 31.8869969551851",
#         geographic_transform=None,
#         Registration_Point=None,
#         in_coor_system='PROJCS["South_America_Albers_Equal_Area_Conic",GEOGCS["GCS_1990_P_b1",DATUM["D_South_American_1969",SPHEROID["GRS_1967",6378160.0,298.25]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["central_meridian",-60.0],PARAMETER["Standard_Parallel_1",-5.0],PARAMETER["Standard_Parallel_2",-42.0],PARAMETER["latitude_of_origin",-32.0],UNIT["Meter",1.0]]',
#         vertical="NO_VERTICAL"
#     )



# this is to clip weird stuff from mspa
# input: "S:\Mikayla\DATA\Projects\AF\NEW_WORKING\masked_test\1991_area_1km_p.tif"
## this input is the mspa_mw_area output but reprojected to have the same coordinate as the binary mask
# # mask: "S:\Mikayla\DATA\Projects\AF\NEW_WORKING\binary_mask\binary_mask_shrink40.tif"
# with arcpy.EnvManager(outputCoordinateSystem='PROJCS["South_America_Albers_Equal_Area_Conic",GEOGCS["GCS_South_American_1969",DATUM["D_South_American_1969",SPHEROID["GRS_1967_Truncated",6378160.0,298.25]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-60.0],PARAMETER["Standard_Parallel_1",-5.0],PARAMETER["Standard_Parallel_2",-42.0],PARAMETER["Latitude_Of_Origin",-32.0],UNIT["Meter",1.0]]', cellSize="1991_area_1km_p.tif"):
#     output_raster = arcpy.sa.RasterCalculator(
#         expression=' Con(IsNull("binary_mask_shrink40.tif"), 0,"1991_area_1km_p.tif")'
#     )
# output_raster.save(r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\masked_test\1991_area_1km_p_con.tif")

# BATCH PROCESSING FUNCTIONS BELOW
import os
import arcpy
import multiprocessing
from functools import partial

# South America Albers projection string
SA_ALBERS = 'PROJCS["South_America_Albers_Equal_Area_Conic",GEOGCS["GCS_South_American_1969",DATUM["D_South_American_1969",SPHEROID["GRS_1967_Truncated",6378160.0,298.25]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-60.0],PARAMETER["Standard_Parallel_1",-5.0],PARAMETER["Standard_Parallel_2",-42.0],PARAMETER["Latitude_Of_Origin",-32.0],UNIT["Meter",1.0]]'

# ArcPy setup
arcpy.env.overwriteOutput = True
arcpy.CheckOutExtension("Spatial")

def reproject_raster(input_raster, output_dir=None, cell_size="31.8869969551851 31.8869969551851", reference_raster=None):
    """Reproject a raster to South America Albers Equal Area Conic"""
    try:
        # Set up output path
        if output_dir is None:
            output_dir = os.path.dirname(input_raster)
        
        base_name = os.path.basename(input_raster)
        output_name = f"{os.path.splitext(base_name)[0]}_p.tif"
        output_raster = os.path.join(output_dir, output_name)
        
        # Use reference raster for cell size if provided
        cell_size_param = reference_raster if reference_raster else cell_size
        
        # Use the exact environment settings from the commented code
        with arcpy.EnvManager(
            outputCoordinateSystem=SA_ALBERS, 
            resamplingMethod="BILINEAR", 
            pyramid="NONE", 
            cellSize=cell_size_param):
            
            print(f"Reprojecting {base_name}...")
            arcpy.management.ProjectRaster(
                in_raster=input_raster,
                out_raster=output_raster,
                out_coor_system=SA_ALBERS,
                resampling_type="BILINEAR",
                cell_size=cell_size,
                geographic_transform=None,
                Registration_Point=None,
                in_coor_system=None,  # Let ArcGIS detect input coordinate system
                vertical="NO_VERTICAL"
            )
        
        print(f"Successfully reprojected: {output_raster}")
        return output_raster
    
    except Exception as e:
        print(f"Error reprojecting {input_raster}: {str(e)}")
        return None

def mask_raster(input_raster, mask_raster, output_dir=None):
    """Mask a raster using Con(IsNull()) to handle NoData values"""
    try:
        # Set up output path
        if output_dir is None:
            output_dir = os.path.dirname(input_raster)
        
        base_name = os.path.basename(input_raster)
        output_name = f"{os.path.splitext(base_name)[0]}_con.tif"
        output_raster = os.path.join(output_dir, output_name)
        
        # Use the exact environment settings from the commented code
        with arcpy.EnvManager(
            outputCoordinateSystem=SA_ALBERS, 
            cellSize=input_raster):
            
            print(f"Masking {base_name} with {os.path.basename(mask_raster)}...")
            expression = f'Con(IsNull("{mask_raster}"), 0, "{input_raster}")'
            out_raster = arcpy.sa.RasterCalculator(expression=expression)
            out_raster.save(output_raster)
        
        print(f"Successfully masked: {output_raster}")
        return output_raster
    
    except Exception as e:
        print(f"Error masking {input_raster}: {str(e)}")
        return None

def process_rasters(process_func, input_dir, use_multiprocessing=False, **kwargs):
    """Process multiple rasters in a directory"""
    arcpy.env.workspace = input_dir
    rasters = arcpy.ListRasters()
    
    if not rasters:
        print(f"No rasters found in directory: {input_dir}")
        return []
    
    print(f"Processing {len(rasters)} rasters...")
    
    if use_multiprocessing:
        # Process multiple files simultaneously
        cores = multiprocessing.cpu_count()
        input_paths = [os.path.join(input_dir, r) for r in rasters]
        func = partial(process_func, **kwargs)
        
        print(f"Using {cores} processes for multiprocessing")
        with multiprocessing.Pool(processes=cores) as pool:
            outputs = pool.map(func, input_paths)
    else:
        # Process files sequentially
        outputs = []
        for raster in rasters:
            input_path = os.path.join(input_dir, raster)
            result = process_func(input_path, **kwargs)
            outputs.append(result)
    
    success_count = sum(1 for p in outputs if p)
    print(f"Process complete: {success_count}/{len(rasters)} succeeded")
    return outputs

# Example usage:
if __name__ == "__main__":
    # Example paths - replace with your actual paths
    input_dir = r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\input_rasters"
    output_dir = r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\output_rasters"
    mask_path = r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\binary_mask\binary_mask_shrink40.tif"
    
    # Uncomment to run batch reprojection
    # reprojected = process_rasters(
    #     reproject_raster, 
    #     input_dir,
    #     use_multiprocessing=True,
    #     output_dir=output_dir,
    #     reference_raster="reference.tif"  # Optional reference raster for cell size
    # )
    
    # Uncomment to run batch masking
    # masked = process_rasters(
    #     mask_raster, 
    #     input_dir,
    #     use_multiprocessing=True,
    #     mask_raster=mask_path,
    #     output_dir=os.path.join(output_dir, "masked")
    # )