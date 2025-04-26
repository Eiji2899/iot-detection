import azure.functions as func
import logging

#adding down
import csv
#from datetime import datetime
#from azure.storage.blob import BlobServiceClient
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
    logging.info(f"Blob trigger processed: {myblob.name}, size: {myblob.length} bytes")

    try:
        content = myblob.read().decode('utf-8').splitlines()
        reader = csv.DictReader(content)

        anomalies = []
        for row in reader:
            try:
                temp = float(row.get('temperature', 0))
                if temp < -50 or temp > 50:  # Example anomaly condition
                    anomalies.append(row)
            except Exception as e:
                logging.warning(f"Skipping invalid row: {row} | Error: {e}")

        if not anomalies:
            logging.info("No anomalies found.")
            return

        # Prepare CSV content manually
        output = "temperature\n"
        for anomaly in anomalies:
            output += f"{anomaly['temperature']}\n"

        # Upload anomaly CSV
        blob_service_client = BlobServiceClient.from_connection_string(iotstorage02123_STORAGE)
        anomaly_blob = blob_service_client.get_blob_client(container=OUTPUT_CONTAINER, blob="anomalies.csv")
        anomaly_blob.upload_blob(output, overwrite=True)

        logging.info(f"Anomalies uploaded successfully. Total: {len(anomalies)}")

    except Exception as e:
        logging.error(f"Error processing blob: {e}")