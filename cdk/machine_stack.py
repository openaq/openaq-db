from aws_cdk import (
    aws_ec2 as _ec2,
    aws_s3 as _s3,
    Stack,
    CfnOutput,
    Duration,
    Tags,
)

from constructs import Construct
import os
import sys


class MachineStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        codeDirectory: str,
        rootVolumeSize: int = 25,
        rootVolumeIops: int = 2000,
        keyName: str = None,
        httpIpRange: str = '0.0.0.0/0',
        sshIpRange: str = None,
        linuxVersion: str = 'amazon_linux_2023',
        instanceType: str = "r5.xlarge",
        vpcId: str = None,
        **kwargs,
    ) -> None:
        """Define stack."""
        super().__init__(scope, id, **kwargs)

        # Get the VPC or create a new one
        if vpcId is not None:
            vpc = _ec2.Vpc.from_lookup(
                self,
                f"{id}-machine-vpc",
                vpc_id=vpcId,
            )
        else:
            vpc = _ec2.Vpc.from_lookup(
               self,
               f"{id}-machine-vpc",
                is_default=True,
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


        initElements = _ec2.CloudFormationInit.from_elements(
            _ec2.InitFile.from_asset("/app/db.zip", setup_dir),
            _ec2.InitCommand.shell_command(
                'cd /app && unzip -o db.zip -d openaqdb'
            ),
            _ec2.InitCommand.shell_command(
                'mkdir -p /var/log/openaq && /app/openaqdb/build_pg16.sh > /var/log/openaq/build_machine.log 2>&1'
            ),
        )

        initOptions = _ec2.ApplyCloudFormationInitOptions(
            # helpful when diagnosing issues
            ignore_failures=True,
            # Optional, how long the installation is expected to take
            # (5 minutes by default)
            timeout=Duration.minutes(120),
        )

        image = _ec2.MachineImage.latest_amazon_linux2023(
            cpu_type=_ec2.AmazonLinuxCpuType.X86_64,
        )

        # user data results are logged to
        # /var/log/cloud-init.log
        # /var/log/cloud-init-output.log
        ec2 = _ec2.Instance(
            self,
            f"{id}-machine-stack-database",
            instance_name=f"{id}-machine-stack-database",
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
            #user_data=UserData
        )


        CfnOutput(
            scope=self,
            id=f"{id}-public-url",
            value=ec2.instance_public_dns_name,
            description="public dns name",
            export_name=f"{id}-public-url")
