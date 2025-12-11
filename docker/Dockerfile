FROM apache/airflow:2.11.0

# Switch to the airflow user (good practice)
USER airflow

# Copy the requirements file
COPY data /data

# Install the packages
RUN pip install -r /data/requirements.txt
RUN pip install custom_plugin==1.0.0 --extra-index-url https://$AZURE_DEVOPS_TOKEN@pkgs.dev.azure.com
RUN pip freeze

