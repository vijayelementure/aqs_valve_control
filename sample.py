event = {
    "data": {
        "jid": 0,
        "devId": "ffffffff-ffff-ffff-ffff-ffffffffffff",
        "vs": {
            "etm": "2024-09-11T10:38:10Z",
            "val": 1
        },
        "dev": "valve_status"
    },
    "meta": {
        "ver": "1.0",
        "requestId": 787889
    }
}

# Accessing the value of 'val'
val = type(event["data"]["vs"]["val"])
val1 = event["meta"]["requestId"]
val2 = event["data"]["devId"]
print(val)
print(val1)
print(val2)
