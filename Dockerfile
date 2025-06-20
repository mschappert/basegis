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