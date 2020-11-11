'''
Python mapper function

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
from io import StringIO
import time

# S3 session 생성
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

# Mapper의 결과가 작성될 S3 Bucket 위치
TASK_MAPPER_PREFIX = "task/mapper/"
SORT_NUM = 10


# 주어진 bucket 위치 경로에 파일 이름이 key인 object와 data를 저장합니다.
def write_to_s3(bucket, key, data, metadata):
    s3.Bucket(bucket).put_object(Key=key, Body=data, Metadata=metadata)


def lambda_handler(event, context):
    start_time = time.time()

    job_bucket = event['jobBucket']
    src_bucket = event['bucket']
    src_keys = event['keys']
    job_id = event['jobId']
    mapper_id = event['mapperId']

    output = {}

    line_count = 0
    err = ''

    # 입력 CSV => 츌력 JSON 포멧
    print('src_key: ', src_keys)

    # 모든 key를 다운로드하고 Map을 처리합니다.
    download_time = 0
    for key in src_keys:
        download_start = time.time()
        response = s3_client.get_object(Bucket=src_bucket, Key=key)
        contents = response['Body'].read()
        download_time += (time.time() - download_start)
        # Map Function
        for line in contents.decode().split('\n')[:-1]:
            line_count += 1
            try:
                data = line.split(',')
                srcIp = data[0][:8]
                for first_num in range(SORT_NUM):
                    if int(srcIp[0]) == first_num:
                        if srcIp[0] not in output:
                            output[srcIp[0]] = {}
                        if srcIp not in output[srcIp[0]]:
                            output[srcIp[0]][srcIp] = 0
                        output[srcIp[0]][srcIp] += float(data[3])
            except Exception as e:
                print('error', e)
    print('mapper_download_time: %s sec' % download_time)

    print('output: ', output)
    time_in_secs = (time.time() - start_time)

    # Mapper의 결과를 전처리, 이후에 S3에 저장
    pret = [len(src_keys), line_count, time_in_secs, err]
    mapper_fname = {}
    for key in output:
        mapper_fname[key] = "%s/%s%s/%s" % (job_id, TASK_MAPPER_PREFIX, key, mapper_id)
    print('mapper_fname: ', mapper_fname)
    metadata = {
        "linecount": '%s' % line_count,
        "processingtime": '%s' % time_in_secs,
        "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    }
    print("metadata", metadata)

    # 이 부분을 efs로 변경 시도 해야 할 듯 함.
    upload_time = 0
    for fname in mapper_fname:
        upload_start = time.time()
        write_to_s3(job_bucket, mapper_fname[fname], json.dumps(output[fname]), metadata)
        upload_time += (time.time() - upload_start)
        write_to_s3(job_bucket, job_id + "/reducer_count/" + fname, '', {})
        write_to_s3(job_bucket, job_id + "/reducer_success/" + 'init', '', {})
    print('mapper_upload_time: %s sec' % upload_time)
    return pret
