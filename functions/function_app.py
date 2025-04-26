import azure.functions as func
import logging

#adding down
import csv
from io import StringIO
from datetime import datetime
from azure.storage.blob import BlobServiceClient
#added up

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


#@app.blob_trigger(arg_name="myblob", path="rawdata/{name}",
#                               connection="iotstorage02123_STORAGE") 
#def BlobTrigger(myblob: func.InputStream):
#    logging.info(f"Python blob trigger function processed blob"
#                f"Name: {myblob.name}"
#                f"Blob Size: {myblob.length} bytes")

OUTPUT_CONTAINER = "anomalies"

@app.blob_trigger(arg_name="myblob", path="rawdata/{name}", connection="iotstorage02123_STORAGE")
def blob_trigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob. "
                 f"Name: {myblob.name} | Size: {myblob.length} bytes")

    try:
        # Read the blob as CSV
        csv_data = myblob.read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_data))

        # Check if 'temperature' column exists
        if 'temperature' not in df.columns:
            logging.warning("No 'temperature' column found in the dataset. Skipping anomaly detection.")
            return

        # Perform Z-score anomaly detection
        df['zscore'] = (df['temperature'] - df['temperature'].mean()) / df['temperature'].std()
        anomalies = df[df['zscore'].abs() > 3]

        if anomalies.empty:
            logging.info("No anomalies detected.")
            return

        # Prepare anomalies CSV
        output_csv = anomalies.to_csv(index=False)

        # Save anomalies to the anomalies container
        blob_service = BlobServiceClient.from_connection_string(iotstorage02123_STORAGE)
        filename = f"anomalies_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
        anomaly_blob = blob_service.get_blob_client(container=OUTPUT_CONTAINER, blob=filename)
        anomaly_blob.upload_blob(output_csv, overwrite=True)

        logging.info(f"Anomalies saved to {filename} with {len(anomalies)} records.")

    except Exception as e:
        logging.error(f"Error during blob processing: {e}")