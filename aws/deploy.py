#!/usr/bin/env python3
"""Create / update / delete the AWS resources for the experiments:  DynamoDB table + IAM role + Lambda function.

  python aws/deploy.py deploy  [--region R] [--function iot-demo] [--table iot-demo-events] [--memory 256]
  python aws/deploy.py status
  python aws/deploy.py destroy [--yes]       # delete function, table, role and log group (stops all cost)
  (to remove resources made under other names: add --function NAME --table NAME --role NAME)

Credentials come from the normal boto3 chain (`aws configure`, ~/.aws/credentials or AWS_* environment variables).
"""
import argparse
import base64
import io
import json
import os
import sys
import time
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parent.parent
ap = argparse.ArgumentParser()
ap.add_argument("action", choices=["deploy", "status", "destroy"])
ap.add_argument("--region", default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-southeast-2")
ap.add_argument("--function", default="iot-demo")
ap.add_argument("--table", default="iot-demo-events")
ap.add_argument("--role", default="iot-demo-role")
ap.add_argument("--memory", type=int, default=256)
ap.add_argument("--yes", action="store_true")
a = ap.parse_args()

sess = boto3.Session(region_name=a.region)
region = sess.region_name
if not region:
    sys.exit("No region set: pass --region (e.g. --region us-east-1) or run `aws configure`.")
try:
    account = sess.client("sts").get_caller_identity()["Account"]
except Exception as e:  # noqa: BLE001
    sys.exit(f"AWS credentials problem: {e}\nRun `aws configure` (or export AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY).")
print(f"account {account}, region {region}")
iam, ddb, lam, logs = sess.client("iam"), sess.client("dynamodb"), sess.client("lambda"), sess.client("logs")
table_arn = f"arn:aws:dynamodb:{region}:{account}:table/{a.table}"


def package():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for f in ("handler.py", "store.py"):
            z.write(ROOT / "lambda_fn" / f, f)
    return buf.getvalue()


def ensure_table():
    try:
        ddb.describe_table(TableName=a.table)
        print(f"table {a.table}: exists")
    except ddb.exceptions.ResourceNotFoundException:
        ddb.create_table(TableName=a.table, BillingMode="PAY_PER_REQUEST",
                         AttributeDefinitions=[{"AttributeName": "device", "AttributeType": "S"},
                                               {"AttributeName": "sk", "AttributeType": "S"}],
                         KeySchema=[{"AttributeName": "device", "KeyType": "HASH"},
                                    {"AttributeName": "sk", "KeyType": "RANGE"}])
        ddb.get_waiter("table_exists").wait(TableName=a.table)
        print(f"table {a.table}: created (on-demand)")


def ensure_role():
    trust = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole",
             "Principal": {"Service": "lambda.amazonaws.com"}}]}
    try:
        arn = iam.get_role(RoleName=a.role)["Role"]["Arn"]
        print(f"role {a.role}: exists")
        fresh = False
    except iam.exceptions.NoSuchEntityException:
        arn = iam.create_role(RoleName=a.role, AssumeRolePolicyDocument=json.dumps(trust))["Role"]["Arn"]
        fresh = True
        print(f"role {a.role}: created")
    iam.attach_role_policy(RoleName=a.role, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")
    iam.put_role_policy(RoleName=a.role, PolicyName="iot-ddb-write", PolicyDocument=json.dumps({
        "Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": ["dynamodb:BatchWriteItem", "dynamodb:PutItem"],
                                                "Resource": table_arn}]}))
    if fresh:
        time.sleep(12)                                   # IAM is eventually consistent
    return arn


def ensure_function(role_arn):
    env = {"Variables": {"STORE_BACKEND": "dynamodb", "DYNAMODB_TABLE": a.table}}
    code = package()
    try:
        lam.get_function(FunctionName=a.function)
        lam.update_function_code(FunctionName=a.function, ZipFile=code)
        lam.get_waiter("function_updated_v2").wait(FunctionName=a.function)
        lam.update_function_configuration(FunctionName=a.function, MemorySize=a.memory, Timeout=30, Environment=env)
        lam.get_waiter("function_updated_v2").wait(FunctionName=a.function)
        print(f"function {a.function}: updated")
    except lam.exceptions.ResourceNotFoundException:
        for attempt in range(8):
            try:
                lam.create_function(FunctionName=a.function, Runtime="python3.12", Role=role_arn, Handler="handler.handler",
                                    Code={"ZipFile": code}, MemorySize=a.memory, Timeout=30, Environment=env,
                                    Architectures=["x86_64"])
                break
            except ClientError as e:
                if "cannot be assumed" in str(e) and attempt < 7:
                    time.sleep(5)
                    continue
                raise
        lam.get_waiter("function_active_v2").wait(FunctionName=a.function)
        print(f"function {a.function}: created ({a.memory} MB, python3.12, x86_64)")


def smoke_test():
    ev = {"run": "deploy-test", "records": [{"id": 1, "device": "dev-000", "ts": time.time(), "temp": 25.0, "hum": 50.0,
                                             "vib": 0.5}], "aggregates": []}
    r = lam.invoke(FunctionName=a.function, Payload=json.dumps(ev).encode(), LogType="Tail")
    body = r["Payload"].read().decode()
    print("smoke test:", "FAILED" if r.get("FunctionError") else "OK", body[:300])
    for line in base64.b64decode(r.get("LogResult", "")).decode(errors="replace").splitlines():
        if line.startswith("REPORT"):
            print(line)
    if r.get("FunctionError"):
        sys.exit(1)


if a.action == "deploy":
    ensure_table()
    ensure_function(ensure_role())
    smoke_test()
    print("\nReady. Quick live comparison:   python demo.py"
          "\nFull experiments (see README):  python run_experiments.py --out results_aws --seeds 1 2 3 --extra --backend aws")
    if (a.function, a.table) != ("iot-demo", "iot-demo-events") or region != "ap-southeast-2":
        print(f"Non-default names/region: add  --aws-function {a.function} --dynamodb-table {a.table} --aws-region {region}  "
              "after --extra (and to demo.py)")
elif a.action == "status":
    for name, fn in (("function", lambda: lam.get_function(FunctionName=a.function)["Configuration"]["State"]),
                     ("table", lambda: ddb.describe_table(TableName=a.table)["Table"]["ItemCount"]),
                     ("role", lambda: iam.get_role(RoleName=a.role)["Role"]["Arn"])):
        try:
            print(f"{name}: {fn()}")
        except ClientError as e:
            print(f"{name}: not found ({e.response['Error']['Code']})")
else:
    if not a.yes and input(f"Delete function '{a.function}', table '{a.table}' (all data) and role '{a.role}'? [y/N] ").lower() != "y":
        sys.exit("aborted")
    for what, call in (
        ("function", lambda: lam.delete_function(FunctionName=a.function)),
        ("log group", lambda: logs.delete_log_group(logGroupName=f"/aws/lambda/{a.function}")),
        ("role policy", lambda: iam.delete_role_policy(RoleName=a.role, PolicyName="iot-ddb-write")),
        ("role policy attachment", lambda: iam.detach_role_policy(
            RoleName=a.role, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")),
        ("role", lambda: iam.delete_role(RoleName=a.role)),
        ("table", lambda: ddb.delete_table(TableName=a.table)),
    ):
        try:
            call()
            print(f"deleted {what}")
        except ClientError as e:
            print(f"skip {what}: {e.response['Error']['Code']}")
    print("Done. (CloudWatch metrics of deleted functions cannot be deleted; they simply expire.)")
