# Welcome to basegis
The goal is to provide a resource for learning how to practically use Docker, and python workflows for people with existing GIS knowledge but are new to implementing Docker, VS Code, GIS in python. This is by no means and exhaustive tutorial, but a way to get up and running.

The Docker container provided contains the basic packages needed for GIS as well as setting up parallel processing, shortening the time, and managing hardware resources for large data processing.

NOTE: 
* This is written for Windows users and may not be the same for MacOS
* Docker Image is about 7 Gb, so make sure you have enough disk space!

# Checklist


* Be aware of your hardware limitations (ram (memory), CPU, disk space)
  * Unsure? - search `system information` in windows search bar

# Getting Started (Initial)
##  Docker Setup

## WSL Setup

## Git Setup

## GitHub Setup

# Creating your own container/project from scratch (not required)
* Checklist: (Bare minimum)
  * Dockerfile
  * environment.yml
  * README.md

* Instructions are written for VS Code
* All files needed to create a container can be made in WSL command line

## 1. Create environment file
* VS Code > R-click > new file > `NAME.yml` (`environment.yml` is often used)
```
name: base_gis
channels:
  - conda-forge
dependencies:
  - jupyterlab=4.2.5
  - pystac-client=0.7.5
  - planetary-computer=1.0.0
  - leafmap=0.38.5
  - matplotlib
  - dask=2024.9.1
  - dask-labextension
  - dask-geopandas=0.4.2
  - distributed=2024.9.1
  - rioxarray=0.17.0
  - xarray=2024.10.0
  - pooch=1.8.2
  - scipy=1.14.1
  - netcdf4=1.7.2
  - geopandas=1.0.1
  - geodatasets=2024.8.0
  - cartopy=0.22.0
  - shapely=2.0.5
  - boto3 #=1.35.54
  - bottleneck
  - stackstac=0.5.0
  - rasterio
```
## 2. Create vim dockerfile
* VS Code > R-click > new file > `Dockerfile`
* No need to add an extension, VS Code will automatically create the Dockerfile
```
FROM continuumio/miniconda3:24.7.1-0

# Copy project files and create environment
COPY environment.yml .
RUN conda env create -f environment.yml

# Activate the Conda environment
RUN echo "conda activate base_gis" >> ~/.bashrc
ENV PATH="$PATH:/opt/conda/envs/base_gis/bin"

# Create a non-root user and switch to that user
RUN useradd -m gisuser
USER gisuser

# Set working directory
WORKDIR /home/gisuser/code/

# Expose the JupyterLab port
EXPOSE 8888
# Expose the Dask Dashboard port
EXPOSE 8787

# Start JupyterLab
CMD ["jupyter", "lab", "--ip=0.0.0.0"]
```
## 4. Build docker image
* Make sure Docker is open on desktop and in WSL by running 'docker'
* Might need to use `docker buildx build` if `docker build` does not work
```
docker build -t <IMAGE_NAME>
```
* Check to see if the image was built
```
docker images
```
## 5. Run docker image
### Option1: General running of image
```
docker run <IMAGE ID or NAME:TAG>
```
### Option 2: Mount and run image
* Connects local machine to image
```
  cd /mnt/c/Users/username/wherever_repository_is/basegis/
  docker run -p 8888:8888 -p 8787:8787 -v $(pwd):/home/gisuser/data/ -it basegis # Do not alter this line
```

# Let's Start Coding!

## Quick Start
1. Open WSL > use cd to wd
```
cd /mnt/c/Users/username/wherever_repository_is/basegis/
```
2. Build Docker Image
```
docker build -t basegis .
```
  1. Make sure docker is open on desktop and in WSL by running `docker` (Docker's menu should show up)
  1. Links dockerfile to the environment file that was created for this project (basegis)
3. Run Image
```
  cd /mnt/c/Users/username/wherever_repository_is/basegis/
  docker run -p 8888:8888 -p 8787:8787 -v $(pwd):/home/gisuser/data/ -it basegis
```
* Need a more detailed step-by-step? Jump down to `Running your Docker container (using basegis)`

## Clone repository



## Running your Docker container (using basegis)
* Make sure docker is open on desktop and in WSL by running `docker` (Docker's menu should show up)
* Windows users may need to open Docker application before initiating Docker in WSL

1. Set working directory (wd) (Example)
```
cd /mnt/c/Users/username/wherever_repository_is/basegis/

```
2. Build image using `environment.yml` file
* Might need to use `docker buildx build` if `docker build` does not work
```
docker build -t basegis .
```
* Check to see if the image was built
```
docker images
```
3. Run Docker image
  * Mount and run image (connects local machine to image)
  * NOTE: You can mount additional locations, however this tutorial will not adderess these nuances
  * DO NOT alter the second line of code (For the sake of this tutorial)
```
cd /mnt/c/Users/username/wherever_repository_is/basegis/

docker run -p 8888:8888 -p 8787:8787 -v $(pwd):/home/gisuser/data/ -it basegis 

```
4. Copy and paste link in browser. If using VS Code see next section for instructions

## Running Jupyter in VS Code
* Copy URL > VS Code > open .ipynb file >
  * Sometimes one link works and the other doesn't, I often try to the last link first (see example below)
```
To access the server, open this file in a browser:
        file#:///home/gisuser/.local/share/jupyter/runtime/jpserver-1-open.html
    Or copy and paste one of these URLs:
        http#://e06ad175546e:8888/lab?token=126f07d295ed5c284b9c7687b2278b579f79a6a2c6e17a24
        http#://129.0.0.1:8888/lab?token=126f07d295ed5c284b9c7687b2278b579f79a6a2c6e17a24
```
* In the upper-right corner of the notebook select `Kernel` >
* Click `Select Another Kernel...` >
* Click `Existing Jupyter Server...`>
* Paste URL from Docker container

# A Few Helpful Commands
See what files are in a folder
```
ls
```

Exit Container (in WSL)
ctrl + C
Then type y or n and hit enter

Stop Docker
```
docker stop <CONTAINER ID or NAME>
```

Remove containers
```
docker rm $(docker ps -aq)
```

# Troubleshooting
Like any software (and GIS in general) things never go as planned, here are some common issues:

Dask
- restart kernel, rerun everything

# Available Workflows
* Pre-processing for MSPA



# Sources/Documentation

## Packages

SELF NOTE
add a sample dataset

this is how to do a link
Data structures: [Link](docs.xarray.dev/en/latest/user-guide/data-structures.html)