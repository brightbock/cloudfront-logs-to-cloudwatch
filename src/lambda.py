import urllib.parse
import botocore
import boto3
import os
import sys
from gzip import GzipFile
from io import BytesIO
from datetime import datetime, date, timedelta
from operator import itemgetter, attrgetter
import time
import json


def logjson(metric, message):
    v = message if (type(message) == dict) else {"msg": "{0}".format(message)}
    print(json.dumps({"metric": metric} | v, separators=(",", ":")))


def logerror(when, error):
    logjson("error", {"msg": str(error), "when": str(when)})


def create_log_stream(log_group_name, log_stream_name):
    for attempt in range(2):
        try:
            logjson("create_log_stream", {"attempt": attempt})
            logs.create_log_stream(logGroupName=log_group_name, logStreamName=log_stream_name)
            return True
        except (
            botocore.exceptions.SSOError,
            botocore.exceptions.ClientError,
            botocore.exceptions.ParamValidationError,
        ) as e:
            logerror("creating log stream", e)
            break
        except Exception as e:
            logerror("creating log stream", e)
            time.sleep(0.1)
    sys.exit(1)


def extract_timestamp(line):
    split_line = line.split(sep="\t")
    t = datetime.strptime("{} {}".format(split_line[0], split_line[1]), "%Y-%m-%d %H:%M:%S").timestamp()
    time_ms = int(float(t) * 1000)
    return time_ms


def line_size(line):
    line_encoded = line.strip().encode("utf-8", "ignore")
    return line_encoded.__sizeof__()


def match_exclusions(line):
    split_line = line.split(sep="\t")
    if match_exclude_sc_status.strip() != "":
        sc_status = split_line[CLOUDFRONT_LOG_FIELD_INDEX["sc-status"]]
        for sc_status_exclude in match_exclude_sc_status.strip().split(sep=","):
            if sc_status.startswith(sc_status_exclude):
                return True
    return False


def cfl_data_to_cwl(data):
    global sequence_token
    records = []
    batch_bytes = 0
    line_timestamp = 0
    earliest_event = 0
    excluded_records = 0

    for line in data.strip().split("\n"):
        if line and not line.startswith("#"):
            try:
                line_timestamp = extract_timestamp(line)
                line_bytes = line_size(line)
            except Exception as e:
                logerror("parsing log line", e)
                continue
            if match_exclusions(line):
                excluded_records += 1
                continue
            if earliest_event <= 0:
                earliest_event = line_timestamp
            if batch_at_limits(len(records), batch_bytes + line_bytes, line_timestamp - earliest_event):
                logjson("put_batch", {"count": len(records), "data_size": batch_bytes})
                sequence_token = put_records_to_cwl(records, sequence_token)
                records = []
                batch_bytes = 0
                earliest_event = 0
            records.insert(len(records), {"timestamp": line_timestamp, "message": line})
            batch_bytes += line_bytes
            if earliest_event > line_timestamp:
                earliest_event = line_timestamp
    if len(records) > 0:
        logjson("put_batch", {"count": len(records), "size": batch_bytes})
        sequence_token = put_records_to_cwl(records, sequence_token)
    logjson("match_exclusions", {"count": excluded_records})


def batch_at_limits(record_count, payload_bytes, time_window):
    event_count = record_count + 1
    if event_count >= 10000:
        return True
    overhead_bytes = event_count * 26
    if (payload_bytes + overhead_bytes) >= 1048576:
        return True
    if time_window >= (86400 * 1000):
        return True
    return False


# Put records and return the next sequence token
def put_records_to_cwl(records, outgoing_sequence_token):
    global log_stream_created

    if not log_stream_created:
        log_stream_created = create_log_stream(log_group_name, log_stream_name)

    new_sequence_token = outgoing_sequence_token

    # records must be ordered by timestamp
    records = sorted(records, key=itemgetter("timestamp"))

    put_log_events_kwargs = {
        "logGroupName": log_group_name,
        "logStreamName": log_stream_name,
        "logEvents": records,
    }
    if outgoing_sequence_token:
        put_log_events_kwargs["sequenceToken"] = outgoing_sequence_token

    for attempt in range(2):
        try:
            # logjson("put_log_events_request", put_log_events_kwargs)
            put_log_events_response = logs.put_log_events(**put_log_events_kwargs)
            new_sequence_token = put_log_events_response["nextSequenceToken"]
            # logjson("put_log_events_response", put_log_events_response)
            break

        except logs.exceptions.InvalidSequenceTokenException as e:
            if e.response["Error"]["Code"] == "InvalidSequenceTokenException":
                if "sequenceToken" in put_log_events_kwargs:
                    del put_log_events_kwargs["sequenceToken"]
                if "expectedSequenceToken" in e.response:
                    put_log_events_kwargs["sequenceToken"] = e.response["expectedSequenceToken"]
                logjson(
                    "put_batch_retry",
                    {"msg": "sequence token fixed", "attempt": attempt},
                )
                continue
            # unexpected, so log & raise
            logerror("put log events", e)
            raise e
        except (logs.exceptions.DataAlreadyAcceptedException) as e:
            if e.response["Error"]["Code"] == "DataAlreadyAcceptedException":
                logjson(
                    "put_batch_already_accepted",
                    {"msg": "batch previously accepted", "attempt": attempt},
                )
                if "expectedSequenceToken" in e.response:
                    new_sequence_token = e.response["expectedSequenceToken"]
                break
            # unexpected, so log & raise
            logerror("put log events", e)
            raise e
        except (
            logs.exceptions.InvalidParameterException,
            logs.exceptions.ResourceNotFoundException,
            logs.exceptions.UnrecognizedClientException,
        ) as e:
            logerror("put log events", e)
            # do not retry these
            break
        except Exception as e:
            logerror("put log events", e)
            continue
    return new_sequence_token


def lambda_handler(event, context):

    # Get our S3 bucket and key from the event context, URL decode the keyname
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(event["Records"][0]["s3"]["object"]["key"], encoding="utf-8")

    # Get S3 object
    logjson("s3_get", {"bucket": bucket, "key": key})
    for attempt in range(2):
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            bytestream = BytesIO(response["Body"].read())
            data = GzipFile(None, "rb", fileobj=bytestream).read().decode("utf-8")
            cfl_data_to_cwl(data)
            break
        except (
            s3.exceptions.NoSuchBucket,
            s3.exceptions.NoSuchKey,
            botocore.exceptions.SSOError,
            botocore.exceptions.ClientError,
        ) as e:
            logerror("get s3 object", e)
            break
        except Exception as e:
            logerror("get s3 object", e)
            continue


CLOUDFRONT_LOG_FIELD_INDEX = {
    "date": 0,
    "time": 1,
    "x-edge-location": 2,
    "sc-bytes": 3,
    "c-ip": 4,
    "cs-method": 5,
    "cs-hdr-host": 6,
    "cs-uri-stem": 7,
    "sc-status": 8,
    "cs-hdr-referer": 9,
    "cs-hdr-user-agent": 10,
    "cs-uri-query": 11,
    "cs-hdr-cookie": 12,
    "x-edge-result-type": 13,
    "x-edge-request-id": 14,
    "x-host-header": 15,
    "cs-protocol": 16,
    "cs-bytes": 17,
    "time-taken": 18,
    "x-forwarded-for": 19,
    "ssl-protocol": 20,
    "ssl-cipher": 21,
    "x-edge-response-result-type": 22,
    "cs-protocol-version": 23,
    "fle-status": 24,
    "fle-encrypted-fields": 25,
    "c-port": 26,
    "time-to-first-byte": 27,
    "x-edge-detailed-result-type": 28,
    "sc-content-type": 29,
    "sc-content-len": 30,
    "sc-range-start": 31,
    "sc-range-end": 32,
}

# Create an Amazon S3 and an Amazon CloudWatch Logs Client
s3 = boto3.client("s3")
logs = boto3.client("logs")
sequence_token = None
log_stream_created = False

# Debug logging
# boto3.set_stream_logger(name='botocore')

# re-use this lambda instance's log stream name in our target log group
log_stream_name = os.getenv(
    "AWS_LAMBDA_LOG_STREAM_NAME",
    "localexecution-{:d}".format(int(float(datetime.now().timestamp()) * 1000)),
)
match_exclude_sc_status = os.getenv("EXCLUDE_SC_STATUS", "")
try:
    log_group_name = os.environ["LOG_GROUP_NAME"]
except KeyError as e:
    logerror("reading environment variable", e)
    sys.exit(1)

logjson("init", {"log_group": log_group_name, "log_stream": log_stream_name})

if __name__ == "__main__":
    context = []
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "BUCKET"},
                    "object": {"key": "cfl-example.gz"},
                }
            }
        ]
    }
    lambda_handler(event, context)
