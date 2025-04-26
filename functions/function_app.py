import azure.functions as func
import logging

#adding down

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
    logging.info(f"Processing blob: {myblob.name} (Size: {myblob.length} bytes)")

    try:
        # Read the CSV data from the blob
        csv_content = myblob.read().decode('utf-8')
        csv_reader = csv.DictReader(StringIO(csv_content))

        temperatures = []
        rows = []

        for row in csv_reader:
            if 'temperature' in row and row['temperature']:
                try:
                    temp = float(row['temperature'])
                    temperatures.append(temp)
                    rows.append(row)
                except ValueError:
                    logging.warning(f"Invalid temperature value: {row['temperature']}")

        if not temperatures:
            logging.warning("No valid temperature data found.")
            return

        # Simple Anomaly Detection: Find values outside reasonable range (example: < -50 or > 50 Celsius)
        anomalies = [row for row in rows if float(row['temperature']) < -50 or float(row['temperature']) > 50]

        if not anomalies:
            logging.info("No anomalies detected.")
            return

        # Prepare anomaly CSV content
        output_csv = StringIO()
        writer = csv.DictWriter(output_csv, fieldnames=anomalies[0].keys())
        writer.writeheader()
        writer.writerows(anomalies)

        # Upload anomalies to storage
        blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN)
        filename = f"anomalies_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
        anomaly_blob = blob_service.get_blob_client(container=OUTPUT_CONTAINER, blob=filename)
        anomaly_blob.upload_blob(output_csv.getvalue(), overwrite=True)

        logging.info(f"Anomalies saved to {filename} with {len(anomalies)} records.")

    except Exception as e:
        logging.error(f"Error processing blob: {e}")