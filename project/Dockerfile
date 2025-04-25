# Use an official Airflow image as base
# Choose a Python version and Airflow version. Match Python version with your local dev if possible.
FROM apache/airflow:2.7.3-python3.10

# Example: Set environment variables if needed (optional)
# ENV AIRFLOW__CORE__LOAD_EXAMPLES=false
# ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=true # Needed if passing complex objects like DataFrames via XCom (though JSON/dict is preferred)

# Switch to root to install dependencies if needed, then back to airflow user
USER root

# Install OS packages if necessary (e.g., for specific database clients)
# RUN apt-get update && apt-get install -y --no-install-recommends some-package && apt-get clean && rm -rf /var/lib/apt/lists/*

# Create output directory and set permissions for airflow user
# The AIRFLOW_UID is set in the docker-compose file usually

RUN mkdir -p /opt/airflow/output && chown "$(id -u airflow):$(id -g airflow)" /opt/airflow/output

# Switch back to the airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python dependencies
# Use constraint file for more robust dependency management, matching the base image's constraints
# Example: Find appropriate constraint file URL from Airflow installation docs/release notes for your version
# ARG AIRFLOW_CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-3.10.txt"
# RUN pip install --no-cache-dir -r /requirements.txt --constraint "${AIRFLOW_CONSTRAINTS}"
# Simpler install without constraints (might lead to conflicts sometimes):
RUN pip install --no-cache-dir -r /requirements.txt

# Copy DAGs and utility scripts
# Ensure the context for `docker build` is the project root directory
COPY dags /opt/airflow/dags

# Copy tests (optional, for running inside container if needed)
#COPY tests /opt/airflow/tests

# Create output directory inside the container (redundant if done as root, but safe)
RUN mkdir -p /opt/airflow/output