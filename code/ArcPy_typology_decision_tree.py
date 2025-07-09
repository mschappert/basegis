# using arcpy to Remap properties
# patch number: 100, 200, 300
# min, maZx, output, nodata
# -70, -1.01, 100, 0
# -1, -0.2, 200, 0 
# -0.1, 0.1, 0, 0
# -0.2, 1, 200, 0
# 1.01, 70, 300, 0

# area number: 10, 20, 30 
# min, maZx, output, nodata
# -70, -1.01, 10, 0
# -1, -0.2, 20, 0
# -0.1, 0.1, 0, 0
# -0.2, 1, 20, 0
# 1.01, 70, 30, 0

# edge number: 1, 2, 3
# min, maZx, output, nodata
# -70, -1.01, 1, 0
# -1, -0.2, 2, 0
# -0.1, 0.1, 0, 0
# -0.2, 1, 2, 0
# -1.01, 70, 3, 0

# then use arcpy to use geoprocessing raster calculator and edge, area, and patch number remap based on the same year 
# (i want to add the values together ie, edge = 1, area = 20, patch = 300, so the final value is 321)

# then make sure there is a raster attribute table with the values

import os
import arcpy
import arcpy.sa
import multiprocessing
import re
import sys
import time


# Parameters
# Remap
input_raster = r""
output_dir = r""
metric_type = "edge" # or "area", "pn"
# Combine Rasters
input_raster = r""
output_dir = r""

def get_year(filename):
    # Extract 4-digit year from filename using regex
    match = re.search(r"(\d{4})", filename)
    return match.group(1) if match else ""

def remap_raster(input_raster, output_dir, stage = metric_type):
    """
    Remap raster values based on the specified stage (patch, area, edge).
    """
    try:
        basename = os.path.basename(input_raster)
        year = get_year(basename)
        
        # Define remap rules based on the stage
        if stage == "pn":
            output_path = os.path.join(output_dir, f"{year}_pn_rmp.tif")
            remap_rules = [
                (-70, -1.01, 100),
                (-1, -0.2, 200),
                (-0.1, 0.1, 0),
                (-0.2, 1, 200),
                (1.01, 70, 300)
            ]
            # Create a remap object
            remap = arcpy.sa.RemapRange(remap_rules)
            # Perform the remapping
            output_raster = arcpy.sa.Reclassify(input_raster, "Value", remap)
        elif stage == "area":
            output_path = os.path.join(output_dir, f"{year}_area_rmp.tif")
            remap_rules = [
                (-70, -1.01, 10),
                (-1, -0.2, 20),
                (-0.1, 0.1, 0),
                (-0.2, 1, 20),
                (1.01, 70, 30)
            ]
            # Create a remap object
            remap = arcpy.sa.RemapRange(remap_rules)
            # Perform the remapping
            output_raster = arcpy.sa.Reclassify(input_raster, "Value", remap)
        elif stage == "edge":
            output_path = os.path.join(output_dir, f"{year}_edge_rmp.tif")
            remap_rules = [
                (-70, -1.01, 1),
                (-1, -0.2, 2),
                (-0.1, 0.1, 0),
                (-0.2, 1, 2),
                (1.01, 70, 3)
            ]
            # Create a remap object
            remap = arcpy.sa.RemapRange(remap_rules)
            # Perform the remapping
            output_raster = arcpy.sa.Reclassify(input_raster, "Value", remap)
        else:
            raise ValueError("Invalid stage specified: {}".format(stage))

        if not arcpy.Exists(output_path):
            output_raster.save(output_path)
            print(f"Remapped successful: {output_path}")
        return output_path
    except Exception as e:
        print(f"Remap error: {str(e)}")
        return None

def combine_by_year(input_dir, output_dir):
    """Automatically combine edge, area, and patch rasters by year."""
    edge_files = [f for f in os.listdir(input_dir) if f.endswith('_edge_rmp.tif')]
    
    for edge_file in edge_files:
        year = get_year(edge_file)
        area_file = f"{year}_area_rmp.tif"
        patch_file = f"{year}_pn_rmp.tif"
        
        edge_path = os.path.join(input_dir, edge_file)
        area_path = os.path.join(input_dir, area_file)
        patch_path = os.path.join(input_dir, patch_file)
        
        if os.path.exists(area_path) and os.path.exists(patch_path):
            output_path = os.path.join(output_dir, f"{year}_combined.tif")
            try:
                combined = arcpy.sa.Raster(edge_path) + arcpy.sa.Raster(area_path) + arcpy.sa.Raster(patch_path)
                combined.save(output_path)
                arcpy.management.BuildRasterAttributeTable(output_path)
                print(f"Combined raster created: {output_path}")
            except Exception as e:
                print(f"Combine error: {str(e)}")

if __name__ == "__main__":
    print("Starting Processing")
    
    # Remap Raster
    print("Starting remapping process...")
    rmp_start = time.time()
    rmp_results = remap_raster(
        input_raster,
        output_dir,
        metric_type
    )
    rmp_duration = time.time() - rmp_start
    print(f"Reamp completed in {rmp_duration:.2f} seconds")
    
    # Combine Rasters
    print("Starting combining process...")
    c_start = time.time()
    combine_by_year(
        output_dir,
        output_dir
    )
    c_duration = time.time() - c_start
    print(f"Combining completed in {c_duration:.2f} seconds")
