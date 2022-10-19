import boto3
import os
from datetime import date, timedelta;


def get_latest_snapshot(description: str ):
    ec2 = boto3.client('ec2')
    snapshot_id = None
    today = date.today()
    yesterday = today - timedelta(days=1)
    descriptions = [
        f'{description}{today}*',
        f'{description}{yesterday}*',
    ]

    rsp = ec2.describe_snapshots(
        Filters=[
            {'Name': 'description', 'Values': descriptions},
            {'Name': 'status', 'Values': ['completed']},
        ],
        MaxResults=5
    )

    if len(rsp['Snapshots']) > 0:
        snapshot_id = sorted(
            rsp['Snapshots'],
            key=lambda d: d['StartTime'],
            reverse=True)[0]['SnapshotId']

    return snapshot_id
