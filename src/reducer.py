'''
Python reducer function

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
import random
import resource
import time

# S3 session 생성
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

# Mapper의 결과가 저장된 S3 Bucket
TASK_MAPPER_PREFIX = "task/mapper/"
# Reducer의 결과를 저장할 S3 Bucket
TASK_REDUCER_PREFIX = "task/reducer/"


# 주어진 bucket 위치 경로에 파일 이름이 key인 object와 data를 저장합니다.
def write_to_s3(bucket, key, data, metadata):
    s3.Bucket(bucket).put_object(Key=key, Body=data, Metadata=metadata)


def lambda_handler(event, context):
    start_time = time.time()

    bucket = event['bucket']
    job_bucket = event['jobBucket']
    job_id = event['jobId']
    r_id = event['reducerId']

    results = {}
    line_count = 0

    # 입력 CSV => 츌력 JSON 포멧

    # 모든 key를 다운로드하고 Reduce를 처리합니다.
    isMapped = False

    paginator = s3_client.get_paginator('list_objects_v2')
    files = []
    download_start = time.time()
    pages = paginator.paginate(Bucket=job_bucket, Prefix=job_id)
    download_time = time.time() - download_start
    print('mapper_download_time: %s sec' % download_time)
    
    for page in pages:
        files += page['Contents']
    for mf in files:
        if "task/mapper/" + str(r_id) in mf["Key"]:
            isMapped = True
            key = mf["Key"]
            response = s3_client.get_object(Bucket=job_bucket, Key=key)
            contents = response['Body'].read()
            try:
                for srcIp, val in json.loads(contents).items():
                    line_count += 1
                    if srcIp not in results:
                        results[srcIp] = 0
                    results[srcIp] += float(val)
            except Exception as e:
                print(e)
    if not isMapped:
        return
    time_in_secs = (time.time() - start_time)
    pret = [len(files), line_count, time_in_secs]
    print("Reducer output", pret)

    metadata = {
        "linecount": '%s' % line_count,
        "processingtime": '%s' % time_in_secs,
        "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    }
    fname = "%s/%s%s" % (job_id, TASK_REDUCER_PREFIX, r_id)
    print('fname: ', fname)

    upload_start = time.time()
    write_to_s3(job_bucket, fname, json.dumps(results), metadata)
    upload_time = time.time() - upload_start
    print('reducer_upload_time: %s sec' % upload_time)

    write_to_s3(job_bucket, job_id + "/reducer_success/" + str(r_id), "", metadata)
    return pret
