# Data Engineering Services for Alterra Academy Capstone Project - Team 7

## ETL Pipeline with Python and Prefect 2.19.4

This project constructs a robust and efficient ETL (Extract, Transform, Load) pipeline using Python. Prefect 2.19.4 orchestrates the workflow and automation of the pipeline, ensuring smooth execution and effective error handling.

## Prerequisites

1.  **Python 3.10+:** Ensure you have Python version 3.10 or later installed.
2.  **Prefect 2.19.4:** Install Prefect using the command `pip install prefect==2.19.4`.
3.  **Prefect Cloud Account:** Create a free account at [https://cloud.prefect.io/](https://cloud.prefect.io/) if you don't have one.
4.  **Prefect Cloud Credentials:** Store your Prefect Cloud API key securely.
5.  **Additional Credentials:**
    *   **Database Credentials:** Prepare credentials to access your database (e.g., username, password, host, database name).
    *   **GitHub Personal Access Token (PAT):** Create a GitHub PAT with appropriate permissions to access your repository.
    *   **GCP Service Account Key:** Create a service account key in Google Cloud Platform with the necessary permissions to access the GCP services you are using.

## Installation

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/capstone-tim-7-alterra/data-pipeline.git
    ```

2.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Prefect Cloud Configuration and Blocks

1.  **Log in to Prefect Cloud:**
    ```bash
    prefect cloud login
    ```
    Enter your API key when prompted.

2.  **Create and Configure Blocks:**
    *   Open Prefect Cloud in your browser.
    *   Navigate to "Blocks".
    *   **Database Blocks:**
        *   Create new blocks of type "Secret" for each database credential (e.g., `db-username`, `db-password`, `db-host`, `db-name`).
        *   Enter the values of your database credentials in the respective blocks.
    *   **GitHub Block:**
        *   Create a new block of type "Secret".
        *   Name it `github-access-token`.
        *   Enter your GitHub PAT.
    *   **GCP Block:**
        *   Create a new block of type "GCP Credentials".
        *   Name it `gcp-service-account`.
        *   Upload your service account key JSON file.

3.  **Create a Work Pool (Optional):**
    *   Open Prefect Cloud in your browser.
    *   Navigate to "Work Pools".
    *   Click "New Work Pool".
    *   Name your work pool (e.g., "etl-pipeline-work-pool").
    *   In the "Pip Packages" section, copy the contents of `requirements.txt` into the provided field.
    *   Save the work pool.

## Deployment and Execution

1.  **Run the Deployment:**
    ```bash
    python code/deployment.py
    ```
    *   This script will create a Prefect deployment for your ETL pipeline.
    *   Ensure your ETL code (`etl_pipeline.py`) reads the configuration from the created blocks.
    *   If you are using a work pool, ensure the deployment uses the work pool you created.

2.  **Monitor in Prefect Cloud:**
    *   Open Prefect Cloud.
    *   You will see your new deployment and can trigger its execution manually or schedule it. 