import logging
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

# JSON Logs (Lambda handles this automatically, the import is for local execution)
try:
    from pythonjsonlogger import jsonlogger

    logger = logging.getLogger()
    logHandler = logging.StreamHandler()
    formatter = jsonlogger.JsonFormatter()
    logHandler.setFormatter(formatter)
    logger.addHandler(logHandler)
except ImportError:
    pass

# Logging Levels
boto3.set_stream_logger(name="botocore", level=logging.WARN)
boto3.set_stream_logger(name="boto3", level=logging.WARN)


def logerror(when, error):
    logging.error("error", extra={"text": str(error), "when": str(when)})


def tsv_to_dict(split_line):
    field_list = CLOUDFRONT_LOG_FIELD_ORDER
    v = {}
    for i, val in enumerate(split_line):
        if i == len(field_list):
            field_list.append(f"UNKNOWN{i}")
        v[field_list[i]] = val
    return v


def create_log_stream(log_group_name, log_stream_name):
    for attempt in range(2):
        try:
            logging.debug("create_log_stream", extra={"attempt": attempt})
            logs.create_log_stream(
                logGroupName=log_group_name, logStreamName=log_stream_name
            )
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


def milliseconds_from_date_time(d, t):
    timestamp = datetime.strptime("{} {}".format(d, t), "%Y-%m-%d %H:%M:%S").timestamp()
    time_ms = int(float(timestamp) * 1000)
    return time_ms


def line_size(line):
    line_encoded = line.strip().encode("utf-8", "ignore")
    return line_encoded.__sizeof__()


def match_exclusions(sc_status):
    if match_exclude_sc_status.strip() != "":
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
            split_line = line.split(sep="\t")
            if len(split_line) < len(CLOUDFRONT_LOG_FIELD_INDEX):
                logerror("parsing log line", {"text": line})
                continue
            if match_exclusions(split_line[CLOUDFRONT_LOG_FIELD_INDEX["sc-status"]]):
                excluded_records += 1
                continue
            line_timestamp = milliseconds_from_date_time(split_line[0], split_line[1])
            if output_json:
                line_to_log = json.dumps(tsv_to_dict(split_line))
            else:
                line_to_log = line
            line_bytes = line_size(line_to_log)
            if earliest_event <= 0:
                earliest_event = line_timestamp
            if batch_at_limits(
                len(records), batch_bytes + line_bytes, line_timestamp - earliest_event
            ):
                logging.info(
                    "put_batch", extra={"count": len(records), "data_size": batch_bytes}
                )
                sequence_token = put_records_to_cwl(records, sequence_token)
                records = []
                batch_bytes = 0
                earliest_event = 0
            records.insert(
                len(records), {"timestamp": line_timestamp, "message": line_to_log}
            )
            batch_bytes += line_bytes
            if earliest_event > line_timestamp:
                earliest_event = line_timestamp
    if len(records) > 0:
        logging.info(
            "put_batch", extra={"count": len(records), "data_size": batch_bytes}
        )
        sequence_token = put_records_to_cwl(records, sequence_token)
    logging.info("match_exclusions", extra={"count": excluded_records})


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
            logging.debug("put_log_events_request", extra=put_log_events_kwargs)
            put_log_events_response = logs.put_log_events(**put_log_events_kwargs)
            new_sequence_token = put_log_events_response["nextSequenceToken"]
            logging.debug("put_log_events_response", extra=put_log_events_response)
            break

        except logs.exceptions.InvalidSequenceTokenException as e:
            if e.response["Error"]["Code"] == "InvalidSequenceTokenException":
                if "sequenceToken" in put_log_events_kwargs:
                    del put_log_events_kwargs["sequenceToken"]
                if "expectedSequenceToken" in e.response:
                    put_log_events_kwargs["sequenceToken"] = e.response[
                        "expectedSequenceToken"
                    ]
                logging.warning(
                    "put_batch_retry",
                    extra={"text": "sequence token fixed", "attempt": attempt},
                )
                continue
            # unexpected, so log & raise
            logerror("put log events", e)
            raise e
        except logs.exceptions.DataAlreadyAcceptedException as e:
            if e.response["Error"]["Code"] == "DataAlreadyAcceptedException":
                logging.warn(
                    "put_batch_already_accepted",
                    extra={"text": "batch previously accepted", "attempt": attempt},
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
    key = urllib.parse.unquote_plus(
        event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )

    # Get S3 object
    logging.info("s3_get", extra={"bucket": bucket, "key": key})
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


CLOUDFRONT_LOG_FIELD_ORDER = [
    "date",
    "time",
    "x-edge-location",
    "sc-bytes",
    "c-ip",
    "cs-method",
    "cs-hdr-host",
    "cs-uri-stem",
    "sc-status",
    "cs-hdr-referer",
    "cs-hdr-user-agent",
    "cs-uri-query",
    "cs-hdr-cookie",
    "x-edge-result-type",
    "x-edge-request-id",
    "x-host-header",
    "cs-protocol",
    "cs-bytes",
    "time-taken",
    "x-forwarded-for",
    "ssl-protocol",
    "ssl-cipher",
    "x-edge-response-result-type",
    "cs-protocol-version",
    "fle-status",
    "fle-encrypted-fields",
    "c-port",
    "time-to-first-byte",
    "x-edge-detailed-result-type",
    "sc-content-type",
    "sc-content-len",
    "sc-range-start",
    "sc-range-end",
]

CLOUDFRONT_LOG_FIELD_INDEX = {}
for i, name in enumerate(CLOUDFRONT_LOG_FIELD_ORDER):
    CLOUDFRONT_LOG_FIELD_INDEX[name] = i

# Create an Amazon S3 and an Amazon CloudWatch Logs Client
s3 = boto3.client("s3")
logs = boto3.client("logs")
sequence_token = None
log_stream_created = False

# re-use this lambda instance's log stream name in our target log group
log_stream_name = os.getenv(
    "AWS_LAMBDA_LOG_STREAM_NAME",
    "localexecution-{:d}".format(int(float(datetime.now().timestamp()) * 1000)),
)
match_exclude_sc_status = os.getenv("EXCLUDE_SC_STATUS", "")
output_json = False if os.getenv("OUTPUT_JSON", "false") == "false" else True
try:
    log_group_name = os.environ["LOG_GROUP_NAME"]
except KeyError as e:
    logerror("reading environment variable", e)
    sys.exit(1)

logging.debug(
    "init", extra={"log_group": log_group_name, "log_stream": log_stream_name}
)

if __name__ == "__main__":
    context = []
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "BUCKET"},
                    "object": {
                        "key": "KEY"
                    },
                }
            }
        ]
    }
    lambda_handler(event, context)
