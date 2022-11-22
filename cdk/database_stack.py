from aws_cdk import (
    aws_ec2 as _ec2,
    Stack,
    CfnOutput,
    Duration,
    Tags,
)

from utils import get_latest_snapshot
from constructs import Construct
import os


class DatabaseStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        codeDirectory: str,
        rootVolumeSize: int = 100,
        rootVolumeIops: int = 2000,
        dataVolumeSize: int = 1000,
        dataVolumeIops: int = 3000,
        elasticIpAllocationId: str = None,
        machineImageName: str = None,
        snapshotId: str = None,
        keyName: str = None,
        expose5432: bool = True,
        expose6432: bool = True,
        expose9187: bool = True,
        httpIpRange: str = '0.0.0.0/0',
        sshIpRange: str = None,
        linuxVersion: str = 'amazon_linux_2',
        instanceType: str = "r5.xlarge",
        databaseReadUser: str = 'postgres',
        databaseReadPassword: str = 'postgres',
        databaseWriteUser: str = 'postgres',
        databaseWritePassword: str = 'postgres',
        databaseMonitorUser: str = 'postgres',
        databaseMonitorPassword: str = 'postgres',
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
                nat_gateways=1,
            )

        # add some security groups
        sg = _ec2.SecurityGroup(
            self,
            f"{id}-dbstack-ssh-sg",
            vpc=vpc,
            allow_all_outbound=True,
        )

        # add an ingress rule for ssh purposes
        if sshIpRange is not None:
            sg.add_ingress_rule(
                peer=_ec2.Peer.ipv4(sshIpRange),
                connection=_ec2.Port.tcp(22)
            )

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

        # Check if we need to lookup the snapshot id
        if snapshotId == 'LATEST':
            snapshotId = get_latest_snapshot('Scalegrid-LCSDatabase-41094-')


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
            "DATABASE_POSTGRES_PASSWORD": databasePostgresPassword,
            "DATABASE_HOST": databaseHost,
            "DATABASE_PORT": databasePort,
            "DATABASE_DB": databaseDb,
            "SNAPSHOT_ID": snapshotId,
            "PGPATH": "/usr/bin",
            "PGDATA": "/db/data",
            "PGCONFIG": "/db/data/postgresql.conf",
        }
        # clear the env file in case it comes from an existing image
        # in which case it already may have values
        UserData.add_commands('> /etc/environment')
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
            UserData.add_commands('mkdir /db && mount /dev/sdb /db')
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
            UserData.add_commands('mkfs -t xfs /dev/sdb && mkdir /db && mount /dev/sdb /db')

        initElements = _ec2.CloudFormationInit.from_elements(
            _ec2.InitFile.from_asset("/app/db.zip", setup_dir),
            _ec2.InitCommand.shell_command(
                'cd /app && unzip -o db.zip -d openaqdb'
            ),
            _ec2.InitCommand.shell_command(
                '/app/openaqdb/install_database.sh > install_database.log 2>&1'
            ),
        )

        initOptions = _ec2.ApplyCloudFormationInitOptions(
            # helpful when diagnosing issues
            ignore_failures=True,
            # Optional, how long the installation is expected to take
            # (5 minutes by default)
            timeout=Duration.minutes(60),
        )

        # Would be nice to add support for a docker method back
        # just would need to add some logic here
        # docker_dir = os.path.join(codeDirectory, 'docker')
        # initElements = _ec2.CloudFormationInit.from_elements(
        #     # Add some files and then build and run the docker image
        #     _ec2.InitFile.from_asset(
        #         "/app/Dockerfile",
        #         os.path.join(docker_dir, 'Dockerfile')
        #     ),
        #     # env data to use for the docker container
        #     _ec2.InitFile.from_asset("/app/env", envPath),
        #     # Because of all the subdirectories its easier just
        #     # to copy everything and unzip it later
        #     _ec2.InitFile.from_asset("/app/db.zip", setup_dir),
        #     # Once we copy the files over we need to
        #     # build and start the instance
        #     # the initfile method does not copy over
        #     # the permissions by default so
        #     # we need to make the init file executable
        #     _ec2.InitCommand.shell_command(
        #         'cd /app && unzip db.zip -d openaqdb && docker build -t db-instance . && docker run --name db-openaq --env-file env --publish 5432:5432 -idt db-instance'
        #     ),
        # )

        # create the instance
        if machineImageName not in [None, '']:
            image = _ec2.MachineImage.lookup(
                name=machineImageName,
            )
        elif linuxVersion == 'ubuntu':
            image = _ec2.MachineImage.from_ssm_parameter(
                '/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id',
            )
            # ubuntu does not come with some needed things
            # so we can add them here
            UserData.add_commands(
                'apt-get update -y',
                'apt-get install -y git awscli ec2-instance-connect',
                'until git clone https://github.com/aws-quickstart/quickstart-linux-utilities.git; do echo "Retrying"; done',
                'cd /quickstart-linux-utilities',
                'source quickstart-cfn-tools.source',
                'qs_update-os || qs_err',
                'qs_bootstrap_pip || qs_err',
                'qs_aws-cfn-bootstrap || qs_err',
                'mkdir -p /opt/aws/bin',
                'ln -s /usr/local/bin/cfn-* /opt/aws/bin/'
            )
        else:
            image = _ec2.MachineImage.latest_amazon_linux(
                generation=_ec2.AmazonLinuxGeneration.AMAZON_LINUX_2,
                cpu_type=_ec2.AmazonLinuxCpuType.X86_64,
            )

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
            block_devices=blockDevices,
            user_data=UserData
        )

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
