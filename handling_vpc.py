import boto3
import time
import datetime
from botocore.exceptions import ClientError
import os

ec2 = boto3.client("ec2")
response = ec2.describe_vpcs()
vpc_id = response["Vpcs"][0]["VpcId"]

#임의의 10개의 VPCs 생성하기
def create_vpc():
    remained_vpc = 10
    while remained_vpc:
        try:
            response = ec2.create_security_group(GroupName = "BigdataEngineering %d trial"%remained_vpc,
            Description = "%d trial"%remained_vpc,
            VpcId=vpc_id,
                )


            security_group_id = response["GroupId"]
            print("Security Group Created %s in vpc %s" % (security_group_id, vpc_id))

            data = ec2.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {'IpProtocol': 'tcp',
                'FromPort': 80,
                'ToPort': 80,
                'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                {'IpProtocol': 'tcp',
                'FromPort': 22,
                'ToPort': 22,
                'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}
                ])


        except ClientError as e:
            f = open("boto3_error_log_while_creating.log", "w")
            f.write("Occured Time : " + str(datetime.datetime.now()) + "\t" + "Occured Error : "+str(e) + "\n")
            f.close()
        remained_vpc -=1

        time.sleep(5)

#생성된 10개의 VPCs를 삭제하기
def delete_vpc():
    remained_vpc = 10
    while remained_vpc:
        try:
            response = ec2.delete_security_group(GroupName = "BigdataEngineering %d trial"%remained_vpc)
            print("Security Group Deleted!")
        except ClientError as e:
            f = open("boto3_error_log_while_removing.log", "w")
            f.write("Occured Time : " + str(datetime.datetime.now()) + "\t" + "Occured Error : "+str(e) + "\n")
            f.close()
        remained_vpc -=1
        time.sleep(5)

#생성된 로그 데이터를 S3에 전송하기
def send_log_data():
    s3 = boto3.client("s3")
    bucket_name= "bigdata-class-hyuna"
    files = os.listdir("./")

    for filename in files:
        if filename.split(".")[-1] == "log":
            s3.upload_file(filename, bucket_name, filename)
