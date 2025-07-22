# Created with Amazon Q
import os
from osgeo import gdal

# Configure GDAL error handling
gdal.UseExceptions()
gdal.PushErrorHandler('CPLQuietErrorHandler')  # Suppress GDAL error messages

def convert_tiff_to_rst(input_file):
    """Convert a single TIFF file to RST format"""
    try:
        output_file = os.path.splitext(input_file)[0] + '.rst'
        
        # Use GDAL's translate utility
        gdal_options = gdal.TranslateOptions(format='RST')
        gdal.Translate(output_file, input_file, options=gdal_options)
        
        print(f"Converted: {os.path.basename(input_file)} -> {os.path.basename(output_file)}")
        return True
        
    except Exception as e:
        print(f"Error converting {input_file}: {str(e)}")
        return False

def convert_folder(input_folder):
    """Convert all TIFF files in a folder to RST format"""
    # Configure GDAL for optimal performance
    gdal.SetConfigOption('GDAL_CACHEMAX', '512')
    gdal.SetConfigOption('GDAL_NUM_THREADS', 'ALL_CPUS')
    
    # Find all TIFF files
    tiff_files = []
    for root, _, files in os.walk(input_folder):
        for file in files:
            if file.lower().endswith(('.tif', '.tiff')):
                tiff_files.append(os.path.join(root, file))
    
    if not tiff_files:
        print("No TIFF files found in the specified folder")
        return
    
    print(f"Found {len(tiff_files)} TIFF files to convert")
    
    # Convert each file
    successful = 0
    for file in tiff_files:
        if convert_tiff_to_rst(file):
            successful += 1
    
    print(f"\nConversion complete: {successful}/{len(tiff_files)} files converted successfully")

def main(file_path=None):
    """Main function to run the converter"""
    # Initialize GDAL
    gdal.AllRegister()
    
    # Check if file_path is a single file or directory
    if file_path:
        if os.path.isfile(file_path):
            if file_path.lower().endswith(('.tif', '.tiff')):
                convert_tiff_to_rst(file_path)
            else:
                print(f"Error: {file_path} is not a TIFF file")
        elif os.path.isdir(file_path):
            convert_folder(file_path)
        else:
            print(f"Error: {file_path} does not exist")
    else:
        print("Please provide a file path or directory")

if __name__ == "__main__":
    # To convert a single file:
    main("path/to/your/file.tif")
    
    # To convert all files in a directory:
    main("path/to/your/directory")
    
    # Or you can run it without arguments and enter the path when prompted
    # import sys
    # if len(sys.argv) > 1:
    #     main(sys.argv[1])
    # else:
    #     path = input("Enter the path to a TIFF file or folder containing TIFF files: ").strip()
    #     main(path)