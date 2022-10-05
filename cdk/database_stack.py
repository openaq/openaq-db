from aws_cdk import (
    aws_ec2 as _ec2,
    Stack,
    CfnOutput,
    Duration,
)

from constructs import Construct
import os


class DatabaseStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        codeDirectory: str,
        rootVolumeSize: int = 25,
        rootVolumeIops: int = 2000,
        dataVolumeSize: int = 1000,
        dataVolumeIops: int = 3000,
        elasticIpAllocationId: str = None,
        snapshotId: str = None,
        keyName: str = None,
        expose5432: bool = True,
        httpIpRange: str = '0.0.0.0/0',
        sshIpRange: str = None,
        instanceType: str = "r5.xlarge",
        databaseReadUser: str = 'postgres',
        databaseReadPassword: str = 'postgres',
        databaseWriteUser: str = 'postgres',
        databaseWritePassword: str = 'postgres',
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

        # Transfer some key data on to the instance
        # must be done in UserData and not as initElements
        # add anything else as needed
        UserData = _ec2.UserData.for_linux()
        data = {
            "DATABASE_READ_USER": databaseReadUser,
            "DATABASE_READ_PASSWORD": databaseReadPassword,
            "DATABASE_WRITE_USER": databaseWriteUser,
            "DATABASE_WRITE_PASSWORD": databaseWritePassword,
            "DATABASE_HOST": databaseHost,
            "DATABASE_PORT": databasePort,
            "DATABASE_DB": databaseDb,
            "SNAPSHOT_ID": snapshotId,
        }

        for key in data:
            value = data[key]
            # this will make sure that they are accessible
            # to all users on the server
            cmd = f'export {key}={value} && echo "{key}=${{{key}}}" >> /etc/environment'
            UserData.add_commands(cmd)

        setup_dir = os.path.join(codeDirectory, 'openaqdb')

        blockDevices = []
        rootVolume = _ec2.BlockDevice(
            device_name="/dev/xvda",
            volume=_ec2.BlockDeviceVolume.ebs(
                rootVolumeSize,
                iops=rootVolumeIops,
                volume_type=_ec2.EbsDeviceVolumeType.IO2
            )
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

        if snapshotId not in [None, '']:
            initElements = None
        else:
            initElements = _ec2.CloudFormationInit.from_elements(
                # Add some files and then build and run the docker image
                # env data to use for the docker container
                # _ec2.InitFile.from_asset("/app/env", envPath),
                # _ec2.InitFile.from_asset("/etc/environment", envPath),
                # Because of all the subdirectories its easier just
                # to copy everything and unzip it later
                _ec2.InitFile.from_asset("/app/db.zip", setup_dir),
                # Once we copy the files over we need to
                # build and start the instance
                # the initfile method does not copy over
                # the permissions by default so
                # we need to make the init file executable
                #
                #_ec2.InitCommand.shell_command(
                #    'cd /app && unzip db.zip -d openaqdb && /app/openaqdb/build.sh'
                #),
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
        ec2 = _ec2.Instance(
            self,
            f"{id}-dbstack-database",
            instance_name=f"{id}-dbstack-database",
            instance_type=_ec2.InstanceType(instanceType),
            machine_image=_ec2.MachineImage.latest_amazon_linux(
                # If we use a next gen AMI we will need to change the
                # cpu type here
                generation=_ec2.AmazonLinuxGeneration.AMAZON_LINUX_2,
                cpu_type=_ec2.AmazonLinuxCpuType.X86_64,
                # generation=_ec2.AmazonLinuxGeneration.AMAZON_LINUX_2022,
                # cpu_type=_ec2.AmazonLinuxCpuType.ARM_64,
            ),
            init=initElements,
            init_options=_ec2.ApplyCloudFormationInitOptions(
                # helpful when diagnosing issues
                ignore_failures=True,
                # Optional, how long the installation is expected to take
                # (5 minutes by default)
                timeout=Duration.minutes(60),
            ),
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
        if elasticIpAllocationId is not None:
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
