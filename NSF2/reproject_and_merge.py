# reproject
import arcpy
import os

input_folder = r"C:\Users\mksch\Desktop\SRTM"
output_folder = r"C:\Users\mksch\Desktop\SRTM\SRTM_P"
reference_raster = r"D:\typology\data\3_MovingWindow\mw_area\1990_area_1km.tif"

# Get spatial reference from reference raster
spatial_ref = arcpy.Describe(reference_raster).spatialReference

arcpy.env.workspace = input_folder
rasters = arcpy.ListRasters("*.tif")
projected_rasters = []

for raster in rasters:
    input_path = os.path.join(input_folder, raster)
    base, ext = os.path.splitext(raster)
    output_name = f"{base}_P{ext}"
    output_path = os.path.join(output_folder, output_name)
    arcpy.management.ProjectRaster(
        input_path,
        output_path,
        spatial_ref,
        "CUBIC"  # Use cubic for DEMs; change if needed
    )
    projected_rasters.append(output_name)
    print(f"Projected: {output_name}")


# merge
# Set workspace to output folder with projected rasters
arcpy.env.workspace = output_folder

# Output mosaic location and name
mosaic_output = os.path.join(output_folder, "SRTM_mosaic.tif")

arcpy.management.MosaicToNewRaster(
    projected_rasters,           # List of projected rasters
    output_folder,               # Output folder
    "mosaic_dem.tif",           # Output raster name
    spatial_ref,                 # Use reference spatial reference
    "32_BIT_FLOAT",             # Pixel type for DEMs
    "",                         # Cell size (optional)
    1,                          # Number of bands (DEM is usually 1)
    "LAST",                    # Mosaic method
    "FIRST"                    # Colormap mode
)
print("Mosaic complete.")
