#!/usr/bin/env python3
from pathlib import Path

import aws_cdk
from aws_cdk import (
    Environment,
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
    f"{settings.ENV}-database",
    codeDirectory=code_dir,
    keyName=settings.KEY_NAME,
    sshIpRange=settings.IP_ADDRESS,
    elasticIpAllocationId=settings.ELASTIC_IP_ALLOCTION_ID,
    snapshotId=settings.SNAPSHOT_ID,
    databaseReadUser=settings.DATABASE_READ_USER,
    databaseReadPassword=settings.DATABASE_READ_PASSWORD,
    databaseWriteUser=settings.DATABASE_WRITE_USER,
    databaseWritePassword=settings.DATABASE_WRITE_PASSWORD,
    databaseHost=settings.DATABASE_HOST,
    databasePort=settings.DATABASE_PORT,
    databaseDb=settings.DATABASE_DB,
)

Tags.of(db).add("Project", settings.ENV)


app.synth()
