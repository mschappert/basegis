import os
import time
import numpy as np
import rasterio
from tqdm import tqdm

def process_raster(input_file, out_path, crs, stage):
    """
    Processes a single raster file: reclassifies data, creates masks, and saves output files.
    Uses NumPy arrays for processing within the file.

    Args:
        input_file (str): Path to the input raster file.
        out_path (str): Path to the output directory.
        crs (str): Coordinate Reference System string.
        stage (str):  "patch", "area", or "edge" - determines reclassification and naming.
    """
    start_time = time.time()

    # Extract filename for output naming
    file_name_base = os.path.splitext(os.path.basename(input_file))[0]

    # Open the input raster file
    with rasterio.open(input_file) as src:
        profile = src.profile  # defines profile
        data = src.read(1)
        print(f"Raster data read successfully: {input_file}")

    # Apply the reclassification function to each pixel based on metric type
    if stage == "patch":
        # Implementation of the classification scheme from the image
        reclassified_data = np.where((data >= -70) & (data <= -1.01), 100,
                            np.where((data >= -1) & (data <= -0.2), 200,
                            np.where((data >= -0.1) & (data <= 0.1), 0,  # NoData range
                            np.where((data >= 0.2) & (data <= 1), 200,
                            np.where((data >= 1.01) & (data <= 70), 300,
                            0)))))  # Values outside specified ranges set to 0
        
        output_values = [300, 200, 100]
        output_names = [f"{file_name_base}_300.tiff", f"{file_name_base}_200.tiff", f"{file_name_base}_100.tiff"]
    elif stage == "area":
        # Same classification scheme but with area-specific values (10, 20, 30)
        reclassified_data = np.where((data >= -70) & (data <= -1.01), 10,
                            np.where((data >= -1) & (data <= -0.2), 20,
                            np.where((data >= -0.1) & (data <= 0.1), 0,  # NoData range
                            np.where((data >= 0.2) & (data <= 1), 20,
                            np.where((data >= 1.01) & (data <= 70), 30,
                            0)))))  # Values outside specified ranges set to 0
                            
        output_values = [30, 20, 10]
        output_names = [f"{file_name_base}_30.tiff", f"{file_name_base}_20.tiff", f"{file_name_base}_10.tiff"]
    elif stage == "edge":
        # Same classification scheme but with edge-specific values (1, 2, 3)
        reclassified_data = np.where((data >= -70) & (data <= -1.01), 1,
                            np.where((data >= -1) & (data <= -0.2), 2,
                            np.where((data >= -0.1) & (data <= 0.1), 0,  # NoData range
                            np.where((data >= 0.2) & (data <= 1), 2,
                            np.where((data >= 1.01) & (data <= 70), 3,
                            0)))))  # Values outside specified ranges set to 0
                            
        output_values = [3, 2, 1]
        output_names = [f"{file_name_base}_3.tiff", f"{file_name_base}_2.tiff", f"{file_name_base}_1.tiff"]

##    if stage == "patch":
##        reclassified_data = np.where(data > 1, 300,
##                            np.where(data < -1, 100,
##                            np.where((data >= -1) & (data <= 1), 200, 0))) # flip 200 and 0
##        output_values = [300, 200, 100]
##        output_names = [f"{file_name_base}_300.tiff", f"{file_name_base}_200.tiff", f"{file_name_base}_100.tiff"]
##    elif stage == "area":
##        reclassified_data = np.where(data > 1, 30,
##                            np.where(data < -1, 10,
##                            np.where((data >= -1) & (data <= 1), 20, 0)))
##        output_values = [30, 20, 10]
##        output_names = [f"{file_name_base}_30.tiff", f"{file_name_base}_20.tiff", f"{file_name_base}_10.tiff"]
##    elif stage == "edge":
##        reclassified_data = np.where(data > 1, 3,
##                            np.where(data < -1, 1,
##                            np.where((data >= -1) & (data <= 1), 2, 0)))
##        output_values = [3, 2, 1]
##        output_names = [f"{file_name_base}_3.tiff", f"{file_name_base}_2.tiff", f"{file_name_base}_1.tiff"]
    else:
        raise ValueError("Invalid stage. Must be 'patch', 'area', or 'edge'.")

    print("Reclass Complete")

    # Create masks for each reclassification category in order to export individually
    masks = []
    for value in output_values:
        masks.append(np.where(reclassified_data == value, value, 0).astype(np.uint8))
    print("Masking Complete")

    # Save each reclassification to separate files
    output_files = dict(zip(output_names, masks))

    for filename, reclass_data in tqdm(output_files.items(), desc="Processing Reclass Files"):
        out_file_path = os.path.join(out_path, filename)
        # Check to see if file exists before saving file
        if os.path.exists(out_file_path):
            os.remove(out_file_path)

        # Write to the output file
        with rasterio.open(out_file_path, 'w', driver='GTiff', height=reclass_data.shape[0],
                           width=reclass_data.shape[1], count=1, dtype=reclass_data.dtype,
                           crs=crs, transform=profile['transform']) as dst:
            dst.write(reclass_data, 1)
            print(f"File saved: {out_file_path}")
    print(f"Reclassified rasters saved for {file_name_base}.")

    end_time = time.time()
    total_time = end_time - start_time
    print(f"Total Elapsed Time for {file_name_base}: {total_time:.2f} seconds")

    return None  # Or return some success/failure indicator if needed


def main():
    """
    Main function to set parameters, define input files, and process them sequentially.
    """
    # Set your parameters
    input_files = {
        "patch": [#"G:/mosaic_reprojected/Overlay_calculation/90-95_pn.tif",
                  #"G:/mosaic_reprojected/Overlay_calculation/95-00_pn.tif",
                  #"G:/mosaic_reprojected/Overlay_calculation/00-05_pn.tif",
                  #"G:/mosaic_reprojected/Overlay_calculation/05-10_pn.tif",
                  #"G:/mosaic_reprojected/Overlay_calculation/10-15_pn.tif",
                  #"G:/mosaic_reprojected/Overlay_calculation/15-20_pn.tif",
                  #"G:/mosaic_reprojected/Overlay_calculation/20-23_pn.tif",
                  "D:/Mikayla_RA/RA_S25/Overlay_to_Standard/pn/90-95_pn_sd.tif",
                  "D:/Mikayla_RA/RA_S25/Overlay_to_Standard/pn/95-00_pn_sd.tif",
                  "D:/Mikayla_RA/RA_S25/Overlay_to_Standard/pn/00-05_pn_sd.tif",
                  "D:/Mikayla_RA/RA_S25/Overlay_to_Standard/pn/05-10_pn_sd.tif",
                  "D:/Mikayla_RA/RA_S25/Overlay_to_Standard/pn/10-15_pn_sd.tif",
                  "D:/Mikayla_RA/RA_S25/Overlay_to_Standard/pn/15-20_pn_sd.tif",
                  "D:/Mikayla_RA/RA_S25/Overlay_to_Standard/pn/20-23_pn_sd.tif"
                  ]#,
        #"area": ["G:/mosaic_reprojected/Overlay_calculation/90-95_area.tif",
                  #"G:/mosaic_reprojected/Overlay_calculation/95-00_area.tif",
                  #"G:/mosaic_reprojected/Overlay_calculation/00-05_area.tif",
                  #"G:/mosaic_reprojected/Overlay_calculation/05-10_area.tif",
                  #"G:/mosaic_reprojected/Overlay_calculation/10-15_area.tif",
                  #"G:/mosaic_reprojected/Overlay_calculation/15-20_area.tif",
                  #"G:/mosaic_reprojected/Overlay_calculation/20-23_area.tif",
                  #],
        #"edge": [#"G:/mosaic_reprojected/Overlay_calculation/90-95_edge.tif",
                  #"G:/mosaic_reprojected/Overlay_calculation/95-00_edge.tif",
                  #"G:/mosaic_reprojected/Overlay_calculation/00-05_edge.tif",
                  #"G:/mosaic_reprojected/Overlay_calculation/05-10_edge.tif",
                  #"G:/mosaic_reprojected/Overlay_calculation/10-15_edge.tif",
                  #"G:/mosaic_reprojected/Overlay_calculation/15-20_edge.tif",
                  #"G:/mosaic_reprojected/Overlay_calculation/20-23_edge.tif",
                  #]
    }

    out_path = "D:/Mikayla_RA/RA_S25/DecisionTree" #"E:/Mikayla_RA" #"G:/mosaic_reprojected/DecisionTree"
    crs = 'PROJCS["SAD_1969_Albers",GEOGCS["SAD69",DATUM["South_American_Datum_1969",SPHEROID["GRS 1967 Modified",6378160,298.2499999999919,AUTHORITY["EPSG","7050"]],AUTHORITY["EPSG","6618"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4618"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["standard_parallel_1",-5],PARAMETER["standard_parallel_2",-42],PARAMETER["latitude_of_center",-32],PARAMETER["longitude_of_center",-60],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]'

    # Ensure the output directory exists
    os.makedirs(out_path, exist_ok=True)

    # Process each file sequentially
    for stage, files in input_files.items():
        print(f"Processing {stage} stage...")
        for input_file in files:
            process_raster(input_file, out_path, crs, stage)


if __name__ == "__main__":
    main()
