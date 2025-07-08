#created with AmazonQ
import os
import glob
from multiprocessing import Pool, cpu_count
from osgeo import gdal

def convert_tiff_to_rst(input_file):
    """Convert a single TIFF file to RST format"""
    try:
        output_file = input_file.replace('.tif', '.rst').replace('.tiff', '.rst')
        
        # Open source dataset
        src_ds = gdal.Open(input_file)
        if src_ds is None:
            print(f"Failed to open {input_file}")
            return False
            
        # Create RST file
        driver = gdal.GetDriverByName('RST')
        dst_ds = driver.CreateCopy(output_file, src_ds)
        
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
    
    # Find all TIFF files
    tiff_patterns = [
        os.path.join(input_folder, "*.tif"),
        os.path.join(input_folder, "*.tiff"),
        os.path.join(input_folder, "**/*.tif"),
        os.path.join(input_folder, "**/*.tiff")
    ]
    
    tiff_files = []
    for pattern in tiff_patterns:
        tiff_files.extend(glob.glob(pattern, recursive=True))
    
    if not tiff_files:
        print("No TIFF files found in the specified folder")
        return
    
    print(f"Found {len(tiff_files)} TIFF files to convert")
    
    # Use all CPU cores if not specified
    if num_processes is None:
        num_processes = cpu_count()
    
    print(f"Using {num_processes} processes for conversion")
    
    # Process files in parallel
    with Pool(processes=num_processes) as pool:
        results = pool.map(convert_tiff_to_rst, tiff_files)
    
    successful = sum(results)
    print(f"\nConversion complete: {successful}/{len(tiff_files)} files converted successfully")

if __name__ == "__main__":
    # Set your input folder path here
    input_folder = input("Enter the folder path containing TIFF files: ").strip()
    
    if not os.path.exists(input_folder):
        print("Folder does not exist!")
    else:
        # Ask for number of cores to use
        cores_input = input(f"Enter number of cores to use (1-{cpu_count()}, or press Enter for all): ").strip()
        num_cores = int(cores_input) if cores_input else None
        
        batch_convert_tiff_to_rst(input_folder, num_cores)
