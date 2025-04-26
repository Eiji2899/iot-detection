import azure.functions as func
import logging
import pandas as pd
import numpy as np
from azure.storage.blob import BlobServiceClient
from io import StringIO
import os
from datetime import datetime

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
    
DATASET_STORAGE_CONN = os.getenv("DatasetStorage")
if not DATASET_STORAGE_CONN:
    logging.error("DatasetStorage connection string not set.")

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
