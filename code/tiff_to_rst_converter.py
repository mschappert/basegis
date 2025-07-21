import os
import glob
import sys
from multiprocessing import Pool, cpu_count
from osgeo import gdal, gdal_array
from osgeo.gdalconst import *

# Configure GDAL error handling
gdal.UseExceptions()
gdal.PushErrorHandler('CPLQuietErrorHandler')  # Suppress GDAL error messages

def convert_tiff_to_rst(input_file):
    """Convert a single TIFF file to RST format using GDAL's native approach"""
    try:
        output_file = os.path.splitext(input_file)[0] + '.rst'
        
        # Use GDAL's translate utility - closest to gdal_translate command line tool
        gdal_options = gdal.TranslateOptions(format='RST')
        gdal.Translate(output_file, input_file, options=gdal_options)
        
        print(f"Converted: {os.path.basename(input_file)} -> {os.path.basename(output_file)}")
        return True
        
    except Exception as e:
        print(f"Error converting {input_file}: {str(e)}")
        return False

def batch_convert_tiff_to_rst(input_folder, num_processes=None):
    """Batch convert all TIFF files in a folder to RST format"""
    
    # Configure GDAL for optimal performance (GDAL native configuration)
    gdal.SetConfigOption('GDAL_CACHEMAX', '512')
    gdal.SetConfigOption('GDAL_NUM_THREADS', 'ALL_CPUS')
    
    # Use GDAL's file finding approach
    vrt_options = gdal.BuildVRTOptions()
    tiff_files = []
    
    # This is how GDAL utilities typically find files
    for root, _, files in os.walk(input_folder):
        for file in files:
            if file.lower().endswith(('.tif', '.tiff')):
                tiff_files.append(os.path.join(root, file))
    
    if not tiff_files:
        print("No TIFF files found in the specified folder")
        return
    
    print(f"Found {len(tiff_files)} TIFF files to convert")
    
    # Use GDAL's thread configuration approach
    if num_processes is None:
        # GDAL typically uses this approach for determining thread count
        num_processes = min(cpu_count(), max(1, int(os.environ.get('GDAL_NUM_THREADS', '4'))))
    
    print(f"Using {num_processes} processes for conversion")
    
    # Process files in parallel
    with Pool(processes=num_processes) as pool:
        results = pool.map(convert_tiff_to_rst, tiff_files)
    
    successful = sum(results)
    print(f"\nConversion complete: {successful}/{len(tiff_files)} files converted successfully")

if __name__ == "__main__":
    # Standard GDAL initialization
    gdal.AllRegister()
    
    # GDAL version info - similar to gdalinfo --version
    print(f"GDAL {gdal.VersionInfo('RELEASE_NAME')}")
    
    # Check driver availability using GDAL's approach
    driver_list = [gdal.GetDriver(i).ShortName for i in range(gdal.GetDriverCount())]
    if 'RST' not in driver_list:
        print("ERROR: RST driver not available in this GDAL installation")
        print("Available drivers: " + ", ".join(driver_list[:5]) + "...")
        sys.exit(1)
    
    # Command-line argument handling similar to GDAL utilities
    if len(sys.argv) > 1:
        input_folder = sys.argv[1]
    else:
        input_folder = input("Enter the folder path containing TIFF files: ").strip()
    
    if not os.path.exists(input_folder):
        print(f"ERROR: Folder '{input_folder}' does not exist!")
        sys.exit(1)
    
    # Process count handling similar to GDAL utilities
    try:
        if len(sys.argv) > 2:
            processes = int(sys.argv[2])
        else:
            processes_input = input("Number of processes to use (press Enter for auto): ").strip()
            processes = int(processes_input) if processes_input else None
    except ValueError:
        processes = None
        
    batch_convert_tiff_to_rst(input_folder, processes)