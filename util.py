import boto3
from faker import Faker
import random
import json
import string
from datetime import datetime

# Create an S3 client object
s3 = boto3.client('s3')

def read_from_s3(last_read_timestamp: str, bucket_name: str, prefix: str):
    # Use a generator expression to filter the S3 objects by last modified timestamp
    paginator = s3.get_paginator('list_objects_v2')
    filtered_objs = (obj for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix) for obj in page["Contents"] \
                    if obj['LastModified'] < last_read_timestamp)
    # Use the `Prefix` parameter in the paginator to filter S3 objects by prefix
    obj_keys_path = [f"s3://{bucket_name}/{obj['Key']}" for obj in filtered_objs if obj['Key'].startswith(prefix)]
    return obj_keys_path


# def generate_log_data(num_lines: int) -> str:
#     fake = Faker()
#     levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
#     log_data = (f"{fake.date_time_between(start_date='-30d', end_date='now').strftime('%Y-%m-%d %H:%M:%S')} "
#                 f"{random.choice(levels)} {fake.text()}" for i in range(num_lines))
#     with open('data/log_data.json', 'w') as outfile:
#         json.dump(list(log_data), outfile)




import json
import random


def generate_log_data(num_lines):
    for i in range(num_lines):
        event_date = datetime.now().strftime("%Y%m%d")
        event_timestamp = f"{int(datetime.now().timestamp() * 1000000)}"
        event_name = random.choice(["push_received", "notification_clicked", "app_opened"])
        event_params = []
        for j in range(random.randint(1, 5)):
            key = "".join(random.choices(string.ascii_lowercase, k=10))
            value_type = random.choice(["string_value", "int_value"])
            value = {"string_value": "".join(random.choices(string.ascii_lowercase, k=10)), "int_value": random.randint(1, 1000)}
            event_params.append({"key": key, "value": {value_type: value[value_type]}})
        event_server_timestamp_offset = f"{random.randint(1, 1000000)}"
        user_pseudo_id = "".join(random.choices(string.ascii_lowercase, k=32))
        user_properties = []
        for k in range(random.randint(1, 10)):
            key = "".join(random.choices(string.ascii_lowercase, k=10))
            value_type = random.choice(["string_value", "int_value"])
            value = {"string_value": "".join(random.choices(string.ascii_lowercase, k=10)), "int_value": random.randint(1, 1000)}
            timestamp_micros = f"{int(datetime.now().timestamp() * 1000000)}"
            user_properties.append({"key": key, "value": {value_type: value[value_type], "set_timestamp_micros": timestamp_micros}})
        user_first_touch_timestamp = f"{int(datetime.now().timestamp() * 1000)}"
        mobile_os_hardware_model = "".join(random.choices(string.ascii_uppercase + string.digits, k=3))
        language = "en-us"
        time_zone_offset_seconds = "-18000"
        version = "11.3.1"
        log_dict = {"event_date": event_date, "event_timestamp": event_timestamp, "event_name": event_name,
                    "event_params": event_params, "event_server_timestamp_offset": event_server_timestamp_offset,
                    "user_pseudo_id": user_pseudo_id, "user_properties": user_properties,
                    "user_first_touch_timestamp": user_first_touch_timestamp,
                    "mobile_os_hardware_model": mobile_os_hardware_model, "language": language,
                    "time_zone_offset_seconds": time_zone_offset_seconds, "version": version}
        yield log_dict

def generate_log_file(num_lines, filename):
    with open(filename, "w") as f:
        for log_dict in generate_log_data(num_lines):
            json.dump(log_dict, f)
            f.write("\n")
