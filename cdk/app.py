#!/usr/bin/env python3
from pathlib import Path
import aws_cdk
from aws_cdk import (
    Tags,
)

import os
from database_stack import DatabaseStack
from settings import settings

code_dir = (Path(__file__).parent.absolute()).parent.absolute()
env_dir = os.path.join(code_dir, f'.env.{settings.ENV}')

app = aws_cdk.App()

db = DatabaseStack(
    app,
    f"openaq-db-{settings.ENV}",
    codeDirectory=code_dir,
    keyName=settings.KEY_NAME,
    sshIpRange=settings.IP_ADDRESS,
    elasticIpAllocationId=settings.ELASTIC_IP_ALLOCTION_ID,
    linuxVersion=settings.LINUX_VERSION,
    snapshotId=settings.SNAPSHOT_ID,
    machineImageName=settings.MACHINE_IMAGE_NAME,
    instanceType=settings.INSTANCE_TYPE,
    databaseReadUser=settings.DATABASE_READ_USER,
    databaseReadPassword=settings.DATABASE_READ_PASSWORD,
    databaseWriteUser=settings.DATABASE_WRITE_USER,
    databaseWritePassword=settings.DATABASE_WRITE_PASSWORD,
    databaseHost=settings.DATABASE_HOST,
    databasePort=settings.DATABASE_PORT,
    databaseDb=settings.DATABASE_DB,
    vpcId=settings.VPC_ID,
    env={
        'account': os.environ['CDK_DEFAULT_ACCOUNT'],
        'region': os.environ['CDK_DEFAULT_REGION']
    }
)

Tags.of(db).add("Project", settings.ENV)

app.synth()
