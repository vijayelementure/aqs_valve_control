import uuid
import pymongo
import os
import time

start_time = time.time()

MONGODB_URI = os.getenv("MONGO_DB_DEV_URI")
DATABASE_NAME = os.getenv("DB_NAME")
AQS_VALVE_LOG = os.getenv("AQS_VALVE_LOG")
AQS_VALVE_STATUS = os.getenv("AQS_VALVE_STATUS")

client = pymongo.MongoClient(
    MONGODB_URI,
    uuidRepresentation="standard",
)
print("Connected To MongoDB Database")


def lambda_handler(event, context):
    db = client[DATABASE_NAME]
    valve_log_coll = db[AQS_VALVE_LOG]
    valve_status_coll = db[AQS_VALVE_STATUS]
    try:
        # threading.Thread(target=handle_database_operation, args=(data,)).start()  # Noqa
        flag_toUpdate = "success" if event["result"] == "SUCCESS" else "failure" # noqa
        request_id = uuid.UUID(event["requestId"])
        device_id = uuid.UUID(event["uuid"])
        flag = flag_toUpdate
        query = {
                "request_id": request_id,
                "device_id": device_id,
                }
        update = {"$set": {"flag": flag}}
        valve_log_coll.update_one(query, update)

        req_op = valve_log_coll.find_one(query)
        print(req_op)

        if flag == "success":
            status_query = {"device_id": device_id}
            status_update = {"$set": {"valve_status": req_op["action"]}}
            valve_status_coll.update_one(status_query, status_update)
        else:
            status_query = {"device_id": device_id}
            status_update = {"$set": {"valve_status": req_op["previous_state"]}} # noqa
            valve_status_coll.update_one(status_query, status_update)
    except Exception as e:
        # Log the error for debugging
        print(f"Error processing the record: {str(e)}")

    return {"statusCode": 200, "message": "status updated successfully"}


end_time = time.time()

delta_time = end_time - start_time


print("time taken: ", delta_time)
