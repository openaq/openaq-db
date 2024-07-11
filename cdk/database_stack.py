from aws_cdk import (
    aws_ec2 as _ec2,
    aws_s3 as _s3,
    Stack,
    CfnOutput,
    Duration,
    Tags,
)

from utils import get_latest_snapshot
from constructs import Construct
import os
import sys


class DatabaseStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        codeDirectory: str,
        pgSharedBuffers: str,
        pgWalBuffers: str,
        pgEffectiveCacheSize: str,
        pgWorkMem: str,
        pgMaintenanceWorkMem: str,
        rootVolumeSize: int = 100,
        rootVolumeIops: int = 2000,
        dataVolumeSize: int = 1000,
        dataVolumeIops: int = 3000,
        privateIpAddress: str = None,
        elasticIpAllocationId: str = None,
        machineImageName: str = None,
        snapshotId: str = None,
        keyName: str = None,
        expose5432: bool = True,
        expose6432: bool = True,
        expose9187: bool = True,
        expose9100: bool = True,
        httpIpRange: str = '10.0.0.0/16',
        devSecurityGroup: str = None,
        transferUri: str = None,
        linuxVersion: str = 'amazon_linux_2023',
        instanceType: str = "r5.xlarge",
        databaseReadUser: str = 'postgres_read',
        databaseWriteUser: str = 'postgres_write',
        databaseMonitorUser: str = 'postgres_monitor',
        databaseReadPassword: str = 'postgres',
        databaseWritePassword: str = 'postgres',
        databaseMonitorPassword: str = 'postgres',
        databasePostgresUser: str = 'postgres',
        databasePostgresPassword: str = 'postgres',
        databaseHost: str = 'localhost',
        databasePort: str = '5432',
        databaseDb: str = 'openaq',
        vpcId: str = None,
        **kwargs,
    ) -> None:
        """Define stack."""
        super().__init__(scope, id, **kwargs)

        # Get the VPC or create a new one
        if vpcId is not None:
            vpc = _ec2.Vpc.from_lookup(
                self,
                f"{id}-dbstack-vpc",
                vpc_id=vpcId,
            )
        else:
            vpc = _ec2.Vpc(
                self,
                f"{id}-dbstack-vpc",
                cidr="10.0.0.0/16",
            )

        # add some security groups
        sg = _ec2.SecurityGroup(
            self,
            f"{id}-dbstack-ssh-sg",
            vpc=vpc,
            allow_all_outbound=True,
        )

        # add an ingress rule for ssh purposes
        #if sshIpRange is not None:
        #    sg.add_ingress_rule(
        #        peer=_ec2.Peer.ipv4(sshIpRange),
        #        connection=_ec2.Port.tcp(22)
        #    )

        # if we want to expose 5432 on the instance
        if expose5432:
            sg.add_ingress_rule(
                peer=_ec2.Peer.ipv4(httpIpRange),
                connection=_ec2.Port.tcp(5432)
            )

        if expose6432:
            sg.add_ingress_rule(
                peer=_ec2.Peer.ipv4(httpIpRange),
                connection=_ec2.Port.tcp(6432)
            )

        if expose9187:
            sg.add_ingress_rule(
                peer=_ec2.Peer.ipv4(httpIpRange),
                connection=_ec2.Port.tcp(9187)
            )

        if expose9100:
	        sg.add_ingress_rule(
                peer=_ec2.Peer.ipv4(httpIpRange),
                connection=_ec2.Port.tcp(9100)
            )

        # Check if we need to lookup the snapshot id
        #if snapshotId == 'LATEST':
        #    snapshotId = get_latest_snapshot('Scalegrid-LCSDatabase-41094-')

        # Transfer some key data on to the instance
        # must be done in UserData and not as initElements
        # add anything else as needed
        UserData = _ec2.UserData.for_linux()
        data = {
            "DATABASE_INSTANCE_ID": id,
            "DATABASE_READ_USER": databaseReadUser,
            "DATABASE_READ_PASSWORD": databaseReadPassword,
            "DATABASE_WRITE_USER": databaseWriteUser,
            "DATABASE_WRITE_PASSWORD": databaseWritePassword,
            "DATABASE_MONITOR_USER": databaseMonitorUser,
            "DATABASE_MONITOR_PASSWORD": databaseMonitorPassword,
            "DATABASE_POSTGRES_USER": databasePostgresUser,
            "DATABASE_POSTGRES_PASSWORD": databasePostgresPassword,
            "DATABASE_HOST": databaseHost,
            "DATABASE_PORT": databasePort,
            "DATABASE_DB": databaseDb,
            "TRANSFER_URI": transferUri,
            "PG_SHARED_BUFFERS": pgSharedBuffers,
            "PG_WAL_BUFFERS": pgWalBuffers,
            "PG_EFFECTIVE_CACHE_SIZE": pgEffectiveCacheSize,
            "PG_WORK_MEM": pgWorkMem,
            "PG_MAINTENANCE_WORK_MEM": pgMaintenanceWorkMem,
            "SNAPSHOT_ID": snapshotId,
            "PGPATH": "/usr/local/pgsql/bin",
            "PGDATA": "/db/data",
            "PGCONFIG": "/db/data/postgresql.conf",
        }
        # clear the env file in case it comes from an existing image
        # in which case it already may have values
        UserData.add_commands('touch -a /etc/environment')
        for key in data:
            value = data[key]
            # Do not export the word `None`
            if value in [None]:
                value = ''
            # this will make sure that they are accessible
            # to all users on the server
            cmd = f'export {key}={value} && echo "{key}=${{{key}}}" >> /etc/environment'
            UserData.add_commands(cmd)

        setup_dir = os.path.join(codeDirectory, 'openaqdb')

        blockDevices = []
        rootVolume = _ec2.BlockDevice(
            device_name="/dev/xvda",
            volume=_ec2.BlockDeviceVolume(
                ebs_device=_ec2.EbsDeviceProps(
                    iops=rootVolumeIops,
                    volume_size=rootVolumeSize,
                    volume_type=_ec2.EbsDeviceVolumeType.IO2,
                )
            ),
        )
        blockDevices.append(rootVolume)

        if snapshotId not in [None, '']:
            snapshotVolume = _ec2.BlockDevice(
                device_name="/dev/sdb",
                volume=_ec2.BlockDeviceVolume(
                    ebs_device=_ec2.EbsDeviceProps(
                        snapshot_id=snapshotId,
                        iops=3000,
                        volume_size=5500,
                        volume_type=_ec2.EbsDeviceVolumeType.IO2,
                    )
                )
            )
            blockDevices.append(snapshotVolume)
            UserData.add_commands('mkdir -p /db && mount /dev/sdb /db')
        else:
            dataVolume = _ec2.BlockDevice(
                device_name="/dev/sdb",
                volume=_ec2.BlockDeviceVolume(
                    ebs_device=_ec2.EbsDeviceProps(
                        iops=dataVolumeIops,
                        volume_size=dataVolumeSize,
                        volume_type=_ec2.EbsDeviceVolumeType.IO2,
                    )
                )
            )
            blockDevices.append(dataVolume)
            UserData.add_commands('mkfs -t xfs /dev/sdb && mkdir -p /db && mount /dev/sdb /db')

        initElements = _ec2.CloudFormationInit.from_elements(
            _ec2.InitFile.from_asset("/app/db.zip", setup_dir),
            _ec2.InitCommand.shell_command(
                'cd /app && unzip -o db.zip -d openaqdb'
            ),
            _ec2.InitCommand.shell_command(
                'mkdir -p /var/log/openaq && /app/openaqdb/install_database.sh > /var/log/openaq/install_database_server.log 2>&1'
            ),
            _ec2.InitCommand.shell_command(
                'sudo -i -u ec2-user IMPORT_FILE_LIMIT=5 /app/openaqdb/initial_setup.sh > /var/log/openaq/initial_setup.log 2>&1'
            ),
        )

        initOptions = _ec2.ApplyCloudFormationInitOptions(
            # helpful when diagnosing issues
            ignore_failures=True,
            # Optional, how long the installation is expected to take
            # (5 minutes by default)
            timeout=Duration.minutes(60),
        )

        # create the instance
        if machineImageName not in [None, '']:
            image = _ec2.MachineImage.lookup(
                name=machineImageName,
            )
        else:
            image = _ec2.MachineImage.latest_amazon_linux2023(
                cpu_type=_ec2.AmazonLinuxCpuType.X86_64,
            )

        # user data results are logged to
        # /var/log/cloud-init.log
        # /var/log/cloud-init-output.log
        ec2 = _ec2.Instance(
            self,
            f"{id}-dbstack-database",
            instance_name=f"{id}-dbstack-database",
            instance_type=_ec2.InstanceType(instanceType),
            machine_image=image,
            init=initElements,
            init_options=initOptions,
            vpc=vpc,
            security_group=sg,
            key_name=keyName,
            vpc_subnets=_ec2.SubnetSelection(
                subnet_type=_ec2.SubnetType.PUBLIC
            ),
            private_ip_address=privateIpAddress,
            block_devices=blockDevices,
            user_data=UserData
        )

        if devSecurityGroup is not None:
            dev_sg = _ec2.SecurityGroup.from_security_group_id(self, "SG", devSecurityGroup, mutable=False)
            ec2.add_security_group(dev_sg)

        backup_bucket = 'openaq-db-backups'
        openaq_backup_bucket = _s3.Bucket.from_bucket_name(
            self, "{env_name}-BACKUP-BUCKET", backup_bucket
        )
        openaq_backup_bucket.grant_read_write(ec2)

        # if we want to assign a specific ip address
        # this can be handy for staging where you
        # may be destroying and rebuilding a lot but
        # not worth it for production
        # where something will be deployed and left alone
        if elasticIpAllocationId not in [None, '']:
            _ec2.CfnEIPAssociation(
                self,
                f"{id}-dbstack-ipaddress",
                allocation_id=elasticIpAllocationId,
                instance_id=ec2.instance_id,
            )

        CfnOutput(
            scope=self,
            id=f"{id}-public-ip",
            value=ec2.instance_public_ip,
            description="public ip",
            export_name=f"{id}-public-ip")


        CfnOutput(
            scope=self,
            id=f"{id}-private-ip",
            value=ec2.instance_private_ip,
            description="private ip",
            export_name=f"{id}-private-ip")


        CfnOutput(
            scope=self,
            id=f"{id}-public-url",
            value=ec2.instance_public_dns_name,
            description="public dns name",
            export_name=f"{id}-public-url")
