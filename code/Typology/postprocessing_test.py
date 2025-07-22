# this code fixes:
# 1. MSPA_preprocessing or MSPA creates weird edges (in this case it was applied post mw)
# 2. repojecting rasters to fit mask
# 3. uses Con to mask out weird edges and leave the image square (making it easier to work with in TerrSet)

# Original code from ArcGIS Pro
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
    
# with arcpy.EnvManager(outputCoordinateSystem='PROJCS["South_America_Albers_Equal_Area_Conic",GEOGCS["GCS_South_American_1969",DATUM["D_South_American_1969",SPHEROID["GRS_1967_Truncated",6378160.0,298.25]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-60.0],PARAMETER["Standard_Parallel_1",-5.0],PARAMETER["Standard_Parallel_2",-42.0],PARAMETER["Latitude_Of_Origin",-32.0],UNIT["Meter",1.0]]', cellSize="1991_area_1km_p.tif"):
#     output_raster = arcpy.sa.RasterCalculator(
#         expression=' Con(IsNull("binary_mask_shrink40.tif"), 0,"1992_area_1km.tif")'
#     )
# output_raster.save(r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\masked_test\1992_area_1km_p_con.tif")


### Made with Amazon Q

import os
import arcpy
import multiprocessing
from functools import partial

##########
# ArcPy Configurations
arcpy.env.overwriteOutput = True
arcpy.env.parallelProcessingFactor = "100%"
arcpy.CheckOutExtension("Spatial")
cores = multiprocessing.cpu_count()

# South America Albers Equal Area Conic projection definition
SA_ALBERS = 'PROJCS["South_America_Albers_Equal_Area_Conic",GEOGCS["GCS_South_American_1969",DATUM["D_South_American_1969",SPHEROID["GRS_1967_Truncated",6378160.0,298.25]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-60.0],PARAMETER["Standard_Parallel_1",-5.0],PARAMETER["Standard_Parallel_2",-42.0],PARAMETER["Latitude_Of_Origin",-32.0],UNIT["Meter",1.0]]'

#########################################    

def init_worker():
    """Initialize ArcPy environment for multiprocessing workers"""
    import arcpy
    arcpy.env.overwriteOutput = True
    arcpy.env.parallelProcessingFactor = "0"
    arcpy.CheckOutExtension("Spatial")
    arcpy.env.pyramid = "NONE"
    arcpy.env.rasterStatistics = "NONE"

def process_rasters(process_func, input_dir, use_multiprocessing=False, **kwargs):
    """Process rasters in batch mode using parallel processing"""
    arcpy.env.workspace = input_dir
    rasters = arcpy.ListRasters()
    
    if not rasters:
        print(f"No rasters found in directory: {input_dir}")
        return []
    
    print(f"Processing {len(rasters)} rasters...")

    if use_multiprocessing:
        arcpy.env.parallelProcessingFactor = "0"
        input_paths = [os.path.join(input_dir, r) for r in rasters]
        func = partial(process_func, **kwargs)
        
        print(f"Using {cores} processes for multiprocessing")
        with multiprocessing.Pool(processes=cores, initializer=init_worker) as pool:
            outputs = pool.map(func, input_paths)
    else:
        arcpy.env.parallelProcessingFactor = "80%"
        outputs = []
        for raster in rasters:
            input_path = os.path.join(input_dir, raster)
            result = process_func(input_path, **kwargs)
            outputs.append(result)
    
    success_count = sum(1 for p in outputs if p)
    print(f"Process complete: {success_count}/{len(rasters)} succeeded")
    return outputs
    
#########################################   

def project_raster(input_raster, output_dir=None):
    """Project raster to South America Albers Equal Area Conic projection"""
    try:
        if output_dir is None:
            output_dir = os.path.dirname(input_raster)
        
        base_name = os.path.basename(input_raster)
        output_name = f"{os.path.splitext(base_name)[0]}_p.tif"
        output_raster = os.path.join(output_dir, output_name)
        
        print(f"Projecting {base_name}...")
        
        with arcpy.EnvManager(outputCoordinateSystem=SA_ALBERS, resamplingMethod="BILINEAR", pyramid="NONE", cellSize=input_raster):
            arcpy.management.ProjectRaster(
                in_raster=input_raster,
                out_raster=output_raster,
                out_coor_system=SA_ALBERS,
                resampling_type="BILINEAR",
                cell_size="31.8869969551851 31.8869969551851",
                geographic_transform=None,
                Registration_Point=None,
                in_coor_system='PROJCS["South_America_Albers_Equal_Area_Conic",GEOGCS["GCS_1990_P_b1",DATUM["D_South_American_1969",SPHEROID["GRS_1967",6378160.0,298.25]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["central_meridian",-60.0],PARAMETER["Standard_Parallel_1",-5.0],PARAMETER["Standard_Parallel_2",-42.0],PARAMETER["latitude_of_origin",-32.0],UNIT["Meter",1.0]]',
                vertical="NO_VERTICAL"
            )
        
        print(f"Successfully projected: {output_raster}")
        return output_raster
    
    except Exception as e:
        print(f"Error projecting {input_raster}: {str(e)}")
        return None

def mask_raster(input_raster, mask_file, output_dir=None):
    """Apply mask to raster using Con tool"""
    try:
        if output_dir is None:
            output_dir = os.path.dirname(input_raster)
        
        base_name = os.path.basename(input_raster)
        output_name = f"{os.path.splitext(base_name)[0]}_con.tif"
        output_raster = os.path.join(output_dir, output_name)
        
        print(f"Masking {base_name}...")
        
        # Create raster objects
        in_raster_obj = arcpy.Raster(input_raster)
        mask_raster_obj = arcpy.Raster(mask_file)
        
        with arcpy.EnvManager(outputCoordinateSystem=SA_ALBERS, cellSize=input_raster):
            # Use Con tool directly instead of RasterCalculator
            out_raster = arcpy.sa.Con(
                arcpy.sa.IsNull(mask_raster_obj),
                0,
                in_raster_obj
            )
            out_raster.save(output_raster)
        
        print(f"Successfully masked: {output_raster}")
        return output_raster
    
    except Exception as e:
        print(f"Error masking {input_raster}: {str(e)}")
        return None

#########################################

if __name__ == "__main__":
    # Configuration
    p_in = r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\MSPA_mw_pn"
    p_out = r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\mw_p"
    con_out = r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\con_tool"
    mask_file = r"S:\Mikayla\DATA\Projects\AF\NEW_WORKING\binary_mask\binary_mask_shrink40.tif"
    
    # Uncomment the process you want to run:
    
    # Process 1: Project rasters
    process_rasters(
        project_raster,
        p_in,
        use_multiprocessing=False,
        output_dir=p_out
    )
    
    # Process 2: Mask rasters
    process_rasters(
        mask_raster,
        p_out,
        use_multiprocessing=True,
        mask_file=mask_file,
        output_dir=con_out
    )