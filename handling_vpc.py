import boto3
import time
import datetime
from botocore.exceptions import ClientError
import os
import pandas as pd
from boto.s3.key import Key
import boto
import botocore

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
            # 생성된 10개의 VPC 정보를 객체로 저장하진 않지만, 생성된 ID와 생성된 시간은 Log 파일을 만들어 csv 또는 log 파일에 저장한다.
            with open("boto3_created_succeed.log", "a", encoding="utf-8") as f:
                f.write("Occured Time : " + str(datetime.datetime.now()) + "\t" + "Created GroupId : "+str("BigdataEngineering %d trial"%remained_vpc) + "\n")
                f.close()

            #VPC의 in/out bound는 임의로 설정
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
            # 잘못된 처리가 발생할 경우, exception 처리를 하고 exception 처리 정보는 log에 기록을 남긴다.
            with open("boto3_error_log_while_creating.log", "a", encoding="utf-8") as f:
                f.write("Occured Time : " + str(datetime.datetime.now()) + "\t" + "Occured Error : "+str(e) + "\n")
                f.close()

        remained_vpc -=1
        # 한개의 VPC를 생성한 후 5초 정도의 시간을 두고 다음 VPC를 생성한다
        time.sleep(5)


#생성된 10개의 VPCs를 삭제하기
def delete_vpc():
    raw_ec2_infos = ec2.describe_security_groups()
    # 기 생성된 VPC 정보를 AWS로 부터 받아와서 VPC groupID를 저장한다.
    group_name_list = [raw_ec2_info["GroupName"] for raw_ec2_info in raw_ec2_infos["SecurityGroups"]]

    while group_name_list:
        group_name = group_name_list.pop(0)
        try:
            # 저장된 groupID를 순차적으로 삭제한다.
            response = ec2.delete_security_group(GroupName = group_name
            )
            with open("boto3_remove_succeed.log", "a", encoding="utf-8") as f:
                # 삭제시 삭제되는 groupID와 삭제된 시간을 이전에 만든 Log 파일에 추가하여 기록한다.
                f.write("Occured Time : " + str(datetime.datetime.now()) + "\t" + "Removed GroupId : "+group_name + "\n")
                f.close()

        except ClientError as e:
            with open("boto3_error_while_removing.log", "a", encoding="utf-8") as f:
                # 잘못된 처리가 발생할 경우, exception 처리를 하고 exception 처리 정보는 log에 기록을 남긴다.
                f.write("Occured Time : " + str(datetime.datetime.now()) + "\t" + "Occured Error : "+str(e) + "\n")
                f.close()

        time.sleep(5)


#생성된 로그 데이터를 S3에 전송하기
def send_log_data():
    s3 = boto3.client("s3")
    response = s3.list_buckets()
    resource = boto3.resource("s3")
    buckets = [bucket['Name'] for bucket in response['Buckets']]
    # 버킷의 이름을 임의로 설정하되, 해당 버킷의 이름이 이미 존재하는지 확인 후 없을 경우에만 새롭게 버킷을 생성합니다.
    if buckets:
        bucket_name = buckets[0]
        files = os.listdir("./")
        for filename in files:
            if filename.split(".")[-1] == "log":
                try:
                    resource.Bucket(bucket_name).download_file(filename, filename.split(".")[0] + "_from_boto_3.log")
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        print("The object does not exist.")
                    else:
                        raise
        # 생성된 파일을 전송하되, 해당 파일이 이미 버킷에 있을 경우, 해당 파일을 다운로드 한후 다운로드된 파일의 내용과 새롭게 생성된 파일을 합쳐서 업로드 합니다.
        # 합칠경우 새롭게 작성된 내용이 뒤로 갈 수 있게 만들어 줍니다.
        for filename in files:
            if filename.split(".")[-1] == "log" and "_from_boto_3" in filename:
                with open(filename.replace("_from_boto_3",""), "r" ,encoding="utf-8") as input_file:
                    with open(filename , "a", encoding="utf-8") as output_file:
                        input_messages = input_file.read()
                        output_file.write(input_messages)
                s3.upload_file(filename, bucket_name, filename.replace("_from_boto_3",""))

            elif filename.split(".")[-1] == "log":
                s3.upload_file(filename, bucket_name, filename)
            else:
                print(filename)
    else:
        bucket_name = "NewHyunaBucket-BigdataEngineering"
        files = os.listdir("./")
        for filename in files:
            if filename.split(".")[-1] == "log":
                s3.upload_file(filename, bucket_name, filename)
