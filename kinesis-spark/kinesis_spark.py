#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
  Consumes messages from a Amazon Kinesis streams and does wordcount.
  This example spins up 1 Kinesis Receiver per shard for the given stream.
  It then starts pulling from the last checkpointed sequence number of the given stream.
  Usage: kinesis_wordcount_asl.py <app-name> <stream-name> <endpoint-url> <region-name>
    <app-name> is the name of the consumer app, used to track the read data in DynamoDB
    <stream-name> name of the Kinesis stream (ie. mySparkStream)
    <endpoint-url> endpoint of the Kinesis service
      (e.g. https://kinesis.us-east-1.amazonaws.com)
  Example:
      # export AWS keys if necessary
      $ export AWS_ACCESS_KEY_ID=<your-access-key>
      $ export AWS_SECRET_KEY=<your-secret-key>
      # run the example
      $ bin/spark-submit -jars external/kinesis-asl/target/scala-*/\
        spark-streaming-kinesis-asl-assembly_*.jar \
        external/kinesis-asl/src/main/python/examples/streaming/kinesis_wordcount_asl.py \
        myAppName mySparkStream https://kinesis.us-east-1.amazonaws.com
  There is a companion helper class called KinesisWordProducerASL which puts dummy data
  onto the Kinesis stream.
  This code uses the DefaultAWSCredentialsProviderChain to find credentials
  in the following order:
      Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY
      Java System Properties - aws.accessKeyId and aws.secretKey
      Credential profiles file - default location (~/.aws/credentials) shared by all AWS SDKs
      Instance profile credentials - delivered through the Amazon EC2 metadata service
  For more information, see
      http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html
  See http://spark.apache.org/docs/latest/streaming-kinesis-integration.html for more details on
  the Kinesis Spark Streaming integration.
"""
from __future__ import print_function

import sys
import json
import boto3

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream


def get_fahr_from_celsius(temp):
    return (temp * 9/5) + 32


def get_celsius_from_fahr(temp):
    return (temp - 32) * 5/9


def calculate_index(T, RH):
    # T: Temperature in Fahrenheit
    # RH: Air Humidity

    HI_S = (1.1 * T) - 10.3 + (0.047 * RH)

    if HI_S < 80:
        HI = HI_S
        return HI

    HI_S = -42.379 + (2.04901523 * T) + (10.14333127 * RH) - (0.22475541 * T * RH) - \
        (6.83783 * 10**-3 * T**2) - (5.481717 * 10**-2 * RH**2) + (1.22874 * 10**-3 * T**2 * RH) + \
        (8.5282 * 10**-4 * T * RH**2) - (1.99 * 10**-6 * T**-6 * T**2 * RH**2)

    if (80 <= T and T <= 112) and RH <= 13:
        HI = HI_S - ((3.25 - (0.25 * RH)) * ((17 - abs(T - 95))/17)**0.5)
        return HI

    if (80 <= T and T <= 87) and RH > 85:
        HI = HI_S + (0.02 * (RH - 85) * (87 * T))
        return HI

    HI = HI_S
    return HI


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(
            "Usage: kinesis_spark.py <app-name> <stream-name> <endpoint-url> <region-name>",
            file=sys.stderr)
        sys.exit(-1)

    sqs = boto3.client('sqs')
    sc = SparkContext(appName="kinesis_spark.py")
    ssc = StreamingContext(sc, 1)
    applicationName, streamName, endpointUrl, regionName = sys.argv[1:]
    print("appname is" + applicationName +
          streamName + endpointUrl + regionName)
    lines = KinesisUtils.createStream(
        ssc, applicationName, streamName, endpointUrl, regionName, InitialPositionInStream.LATEST, 2)

    lines.pprint()

    def add_index_to_obj(data: string):
        station_infos = json.loads(data)
        T = get_fahr_from_celsius(station_infos.get('TEMP_MED'))
        RH = station_infos.get("UMID_MED")

        index_in_fahr = calculate_index(T, RH)
        station_infos["HI"] = get_celsius_from_fahr(index_in_fahr)

        return json.dumps(station_infos)

    parsed_with_index = lines.map(lambda data: add_index_to_obj(data))
    parsed_with_index.pprint()

    def handler(rdd):
        # Foward to SQS
        sqs_url = 'https://sqs.us-east-1.amazonaws.com/494824362413/AWS-ElasticMapReduce-j-2KXGR8ZEW2VF'
        message = rdd.collect()
        sqs.send_message(QueueUrl=sqs_url, MessageBody=str(message))

    parsed_with_index.foreachRDD(lambda rdd: handler(rdd))

    parsed_with_index.pprint()

    ssc.start()
    ssc.awaitTermination()
