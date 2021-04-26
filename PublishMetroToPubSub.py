import os
import time 
from google.cloud import pubsub_v1
from google.transit import gtfs_realtime_pb2
import requests
from datetime import datetime, timedelta
import json
import argparse

parser = argparse.ArgumentParser()

""" 
python PublishMetroToPubSub.py --project my-project-name --pubsub_topic projects/my-project-name/topics/transactions --svc_acc_path MyPathToGCPSVCAccPath.json --api_key myapikeyhere
"""

parser.add_argument('--project',
                      dest='project',
                      required=True,
                      help='Name of GCP Project.')
parser.add_argument('--pubsub_topic',
                      dest='pubsub_topic',
                      required=True,
                      help='Topic to post messages to.')
parser.add_argument('--svc_acc_path',
                      dest='svc_acc_path',
                      required=True,
                      help='GCP Service Account Path.')
parser.add_argument('--api_key',
                      dest='api_key',
                      required=True,
                      help='API Key Value for TfNSW Open Data Hub.')

path_args, pipeline_args = parser.parse_known_args()

arg_project = path_args.project
arg_pubsub_topic = path_args.pubsub_topic
arg_svc_acc_path = path_args.svc_acc_path
arg_api_key = path_args.api_key

def convert_dt_to_unix_ts(datetimeval):
    return int(time.mktime(datetimeval.timetuple()))

if __name__ == "__main__":

    ################################# Replace 'my-project' with your project id
    project = arg_project

    ################################# Replace 'my-topic' with your pubsub topic
    pubsub_topic = arg_pubsub_topic

    ################################# Replace 'my-service-account-path' with your service account path
    path_service_account = arg_svc_acc_path
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account    

    # create publisher
    publisher = pubsub_v1.PublisherClient()

    ################################# REPLACE WITH ARGS
    headers = {
        'Accept': 'application/x-google-protobuf',
        'Authorization': 'apikey '+arg_api_key,
    }
    
    # make 10 requests as a demo
    for x in range(1,10):
        print(x)
        response = requests.get('https://api.transport.nsw.gov.au/v1/gtfs/realtime/metro', headers=headers)
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        for entity in feed.entity:
          response_unixtimestamp = convert_dt_to_unix_ts(datetime.strptime(entity.id[0:14], '%Y%m%d_%H%M%S'))
          if entity.HasField('trip_update') and entity.trip_update.vehicle.id:
            tripStartDateTime = datetime.strptime(entity.trip_update.trip.start_date+entity.trip_update.trip.start_time, '%Y%m%d%H:%M:%S')
            stopId=0
            arrivalDelayLatest=0
            arrivalTimeLatest=None
            departureDelayLatest=0
            departureTimeLatest=None
            for stu in entity.trip_update.stop_time_update:
                if datetime.fromtimestamp(stu.departure.time) > (datetime.today() + timedelta(days=-2)):
                    stopId=stu.stop_id
                    arrivalDelayLatest=stu.arrival.delay
                    arrivalTimeLatest=datetime.fromtimestamp(stu.arrival.time).strftime('%Y-%m-%d %H:%M:%S')
                    departureDelayLatest=stu.departure.delay
                    departureTimeLatest=datetime.fromtimestamp(stu.departure.time).strftime('%Y-%m-%d %H:%M:%S')
            json_str = {
                "response_ts":response_unixtimestamp,
                "vehicle_id":entity.trip_update.vehicle.id,
                 "vehicle_label": entity.trip_update.vehicle.label,
                 "trip_start_dt": tripStartDateTime.strftime('%Y-%m-%d %H:%M:%S'),
                 "stop_id": stopId,
                 "latest_arrival_dt": arrivalTimeLatest,
                 "latest_arrival_delay": arrivalDelayLatest,
                 "latest_departure_dt": departureTimeLatest,
                 "latest_departure_delay": departureDelayLatest
                 }
            event_data = json.dumps(json_str).encode("utf-8")
            print('Publishing {0} to {1}'.format(event_data, pubsub_topic))
            publisher.publish(pubsub_topic, event_data)
            time.sleep(0.5)   
 