import azure.functions as func
import logging
import pandas as pd
import numpy as np
import os
from azure.storage.blob import BlobServiceClient
from io import StringIO
from datetime import datetime

app = func.FunctionApp()

# Load connection string for custom dataset storage
DATASET_STORAGE_CONN = os.getenv("DatasetStorage")
OUTPUT_CONTAINER = "anomalies"

@app.blob_trigger(arg_name="myblob", path="rawdata/{name}", connection="DatasetStorage")
def blob_trigger(myblob: func.InputStream):
    logging.info(f"Triggered blob: {myblob.name} ({myblob.length} bytes)")

    try:
        # Read and parse the blob data
        csv_data = myblob.read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_data))

        if 'temperature' not in df.columns:
            logging.warning("No 'temperature' column found in the dataset. Skipping.")
            return

        # Z-score based anomaly detection
        df['zscore'] = (df['temperature'] - df['temperature'].mean()) / df['temperature'].std()
        anomalies = df[df['zscore'].abs() > 3]

        if anomalies.empty:
            logging.info("No anomalies found.")
            return

        # Convert anomalies to CSV
        output_csv = anomalies.to_csv(index=False)

        # Save anomalies with a timestamped filename
        blob_service = BlobServiceClient.from_connection_string(DATASET_STORAGE_CONN)
        filename = f"anomalies_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
        anomaly_blob = blob_service.get_blob_client(container=OUTPUT_CONTAINER, blob=filename)
        anomaly_blob.upload_blob(output_csv, overwrite=True)

        logging.info(f"Anomalies saved to {filename} with {len(anomalies)} entries.")

    except Exception as e:
        logging.error(f"Error during processing: {e}")
