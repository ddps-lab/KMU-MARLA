'''
REDUCER Coordinator 

* Copyright 2016, Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* Licensed under the Amazon Software License (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
* http://aws.amazon.com/asl/
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License. 
'''

import boto3
import json
import lambdautils
import random
import re
import time
import urllib

DEFAULT_REGION = "us-east-1"

### STATES 상태 변수
MAPPERS_DONE = 0
REDUCER_STEP = 1

# S3 session 생성
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
# Lambda session 생성
lambda_client = boto3.client('lambda')
SORT_NUM = 10


# 주어진 bucket 위치 경로에 파일 이름이 key인 object와 data를 저장합니다.
def write_to_s3(bucket, key, data, metadata):
    s3.Bucket(bucket).put_object(Key=key, Body=data, Metadata=metadata)

# mapper의 파일 개수를 카운트 합니다. 파일 개수가 reducer의 step 수를 결정
def get_mapper_files(files, sort_num):
    ret = []
    for i in range(sort_num):
        for mf in files:
            idx = str(i)
            if "task/mapper/" + idx in mf["Key"]:
                ret.append(mf)
        if len(ret) > 0:
            break
    return len(ret)

def lambda_handler(event, context):
    start_time = time.time()

    # Job Bucket으로 이 Bucket으로부터 notification을 받습니다.
    bucket = event['Records'][0]['s3']['bucket']['name']
    config = json.loads(open('./jobinfo.json', "r").read())

    job_id = config["jobId"]
    map_count = config["mapCount"]
    r_function_name = config["reducerFunction"]
    r_handler = config["reducerHandler"]

    ### Mapper 완료된 수를 count 합니다. ###

    # Job 파일들을 가져옵니다.
    paginator = s3_client.get_paginator('list_objects_v2')

    files = []
    pages = paginator.paginate(Bucket=bucket, Prefix=job_id)
    for page in pages:
        files += page['Contents']

    mapper_keys_length = get_mapper_files(files, SORT_NUM)
    print("Mappers Done so far ", mapper_keys_length)

    if map_count == mapper_keys_length:

        # 모든 mapper가 완료되었다면, reducer를 시작합니다.

        for i in range(SORT_NUM):
            # Reducer Lambda를 비동기식(asynchronously)으로 호출(invoke)합니다.
            resp = lambda_client.invoke(
                FunctionName=r_function_name,
                InvocationType='Event',
                Payload=json.dumps({
                    "bucket": bucket,
                    "jobBucket": bucket,
                    "jobId": job_id,
                    "reducerId": i
                })
            )
            print(resp)
        return
    else:
        print("Still waiting for all the mappers to finish ..")
