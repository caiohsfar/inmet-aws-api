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
import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
import requests

#import #logging
##logging.basicConfig(filename='app.log', level=#logging.INFO)
##logging.info("App loaded")

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

    HI_S = -42.379 + (2.04901523 * T) + (10.14333127 * RH) + (-0.22475541 * T * RH) + \
        (-6.83783e-03 * T**2) + (-5.481717e-02 * RH**2) + (1.22874e-03 * T**2 * RH) + \
        (8.5282e-04 * T * RH**2) + (-1.99e-06 * T**2 * RH**2)


    if (80 <= T and T <= 112) and RH <= 13:
        HI = HI_S - ((3.25 - (0.25 * RH)) * ((17 - abs(T - 95))/17)**0.5)
        return HI

    if (80 <= T and T <= 87) and RH > 85:
        HI = HI_S + (0.02 * (RH - 85) * (87 * T))
        return HI

    HI = HI_S
    return HI

stations_access_tokens = {
    "A309": "TJNf5MXLN5SXYMhyDzbI",
    "A329": "e2DhPbA3fbUO2t3CiKuT",
    "A341": "yJNlve5MZqM7E6l7S4Bs",
    "A351": "ds4V0k621MZJtXRtBLdo",
    "A322": "fSiGfNjfx3iwxXnVQRdb",
    "A349": "zAqnW1PG0ugYkfC2ArF2",
    "A366": "KpEWpgMpAf5VEL4Mowhe",
    "A357": "nXGDA4BiJRRJzVakWoUu",
    "A307": "vaJHEJwiKREULXMdp1G5",
    "A301": "N6igplOEhdw6fmplb6t5",
    "A370": "tHD8qU3GZfm2DJ4jIORC",
    "A350": "V9tx8s8lH2x6o4wijsRv",
    "A328": "fC9FuuKzc4iaWahjzZjA",
    "82890": "b5RZ75xAuU3GPWaDNHXy",
    "82886": "5GeZuTfxZiNhW9jp2sY2",
    "82893": "R2e7m3m0VrI5cBHzgK8E",
    "82753": "E0sFwaULmeO85FnvN5IL",
    "82983": "jvCxrmUWqt5Z5FGjhJ2D",
    "82900": "SR1PbVazj3rI6dSuHbhC",
    "82797": "LYz18XSt0nNb3tDV8X9J",
}

tb_host = "demo.thingsboard.io"

def send_data_to_tb(station_key_value):
    #logging.info("station_key_value" + str(station_key_value))
    station_key = next(iter(station_key_value))
    #logging.info("station_key" + str(station_key))


    device_token = stations_access_tokens[station_key]
    station_value = station_key_value[station_key]
    
    as_array = [station_value]
    try:
        url = 'http://' + tb_host + '/api/v1/' + device_token + '/telemetry'
        response = requests.post(url, json=as_array)
        #logging.info("RESPONSE" + str(response))
    except ConnectionError as e:
        #logging.error("ERROR RESPONSE" + str(e))
        print(e)


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
    stream = KinesisUtils.createStream(
        ssc, applicationName, streamName, endpointUrl, regionName, InitialPositionInStream.LATEST, 2)

    stream.pprint()

    def get_hi_state(hi):
        hi = float(hi)
        if hi <= 27:
            return "NORMAL"
        elif hi >= 27.1 and hi <= 32:
            return "CAUTELA"
        elif hi >= 32.1 and hi <= 41:
            return "CAUTELA EXTREMA"
        elif hi >= 41.1 and hi <= 54:
            return "PERIGO"
        elif hi > 54:
            return "PERIGO EXTREMO"
        else:
            return "N/A"

    def add_hi_to_obj_and_adjust_values(data):
        station_infos = json.loads(data)
        key_station = next(iter(station_infos))

        station_infos[key_station]

        temp_med = station_infos[key_station]["TEMP_MED"]
        umid_med = station_infos[key_station]["UMID_MED"]

        temp_min = station_infos[key_station]["TEMP_MIN"]
        temp_max = station_infos[key_station]["TEMP_MAX"]
        
        if temp_med == None:
            if None not in (temp_min, temp_max):
                calc_temp_med = (float(temp_max) + float(temp_min))/ 2
                temp_med = float("{:.2f}".format(calc_temp_med))
                station_infos[key_station]["TEMP_MED"] = temp_med
            
        
        if None not in (temp_med, umid_med):
            T = get_fahr_from_celsius(float(temp_med))
            RH = float(umid_med)
            index_in_fahr = calculate_index(T, RH)
            station_infos[key_station]["HI"] = float("{:.2f}".format(get_celsius_from_fahr(index_in_fahr)))
            station_infos[key_station]["HI_ALERTA"] = get_hi_state(station_infos[key_station]["HI"])
            
        else:
            station_infos[key_station]["HI"] = "N/A"
            station_infos[key_station]["HI_ALERTA"] = "N/A"

        
        for (key, value) in station_infos[key_station].items():
            if value == None:
                station_infos[key_station][key] = "N/A"


        return json.dumps(station_infos)

    parsed_with_index = stream.map(lambda data: add_hi_to_obj_and_adjust_values(data))
    parsed_with_index.pprint()

    def handler(rdd):
        # Foward to ThingsBoard
        stations = rdd.collect()
        for station in stations:
            #logging.info("STATION IN LOOP DATA " + str(station))
            send_data_to_tb(json.loads(station))




    parsed_with_index.foreachRDD(lambda rdd: handler(rdd))

    parsed_with_index.pprint()

    ssc.start()
    ssc.awaitTermination()
