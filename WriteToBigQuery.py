import apache_beam as beam
from apache_beam import window, transforms
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse
import os
from google.cloud import bigquery
import json
from datetime import datetime
import time

parser = argparse.ArgumentParser()
"""
python WriteToBigQuery.py --input projects/my-gcp-project/subscriptions/Subscriber1 --output my-gcp-project:ds_metro --temp_location gs://my-gcp-bucket/temp_dataflowdemo --svc_acc_path MyPathToGCPSVCAccPath.json 
"""

parser.add_argument('--input',
                      dest='input',
                      required=True,
                      help='Input file to process.')
parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output table to write results to.')
parser.add_argument('--temp_location',
                      dest='temp_location',
                      required=True,
                      help='Temporary location for GCP to write data to.')
parser.add_argument('--svc_acc_path',
                      dest='svc_acc_path',
                      required=True,
                      help='GCP Service Account Path.')

path_args, pipeline_args = parser.parse_known_args()

inputs_pattern = path_args.input
outputs_prefix = path_args.output
temp_location = path_args.temp_location
arg_svc_acc_path = path_args.svc_acc_path

pipeline_options = PipelineOptions(pipeline_args)
pipeline_options.view_as(StandardOptions).streaming = True


################################# Replace 'my-service-account-path' with your service account path
service_account_path = arg_svc_acc_path
print("Service account file : ", service_account_path)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path

def to_json(row):
    json_output =json.loads(row)
    return json_output

# The DoFn to perform on each element in the input PCollection.
class CollectVehicleDelays(beam.DoFn):
    def process(self, element):
        # Returns a list of tuples containing stop and departure delay
        result = [(element['vehicle_id'], element['latest_arrival_delay'])]
        return result

def convert_dt_to_unix_ts(datetimeval):
    return int(time.mktime(datetimeval.timetuple()))

def transform_to_json(element):
    return {'vehicle_id':element[0],'avg_arrival_delay':element[1], 'processing_ts': convert_dt_to_unix_ts(datetime.now())}
 
# BigQuery
# Construct a BigQuery client object.
client = bigquery.Client() 

# Load to BigQuery
table_veh_info_schema={"fields":[{"type":"STRING","name":"vehicle_id","mode":"REQUIRED"},
                                {"type":"FLOAT","name":"avg_arrival_delay","mode":"REQUIRED"},
                                {"type":"INT64","name":"processing_ts","mode":"REQUIRED"}]}
table_name_veh_info='.tbl_metro_veh_info'   
    
with beam.Pipeline(options=pipeline_options) as p:
    pubsub_data = (
        p
        | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription= inputs_pattern).with_output_types(bytes)
        | 'Decode from bytes' >> beam.Map(lambda x: x.decode('utf-8'))
        | 'Convert string to json' >> beam.Map(to_json)
        | 'Filter Only Trains At Stops' >> beam.Filter(lambda x : (x['stop_id']!='0'))
        | 'Identify Field For Use In Windowing' >> beam.Map(lambda x: beam.window.TimestampedValue(x, x['response_ts']))
        | beam.WindowInto(beam.window.FixedWindows(1 * 60), 
                        trigger=beam.transforms.trigger.Repeatedly( 
                        beam.transforms.trigger.AfterAny(beam.transforms.trigger.AfterCount(5), 
                        beam.transforms.trigger.AfterProcessingTime(1 * 60))), 
                        accumulation_mode=beam.transforms.trigger.AccumulationMode.DISCARDING,
                        allowed_lateness=60)
        | 'Create tuples from vehicle and delay time' >> beam.ParDo(CollectVehicleDelays())
        | 'Group by the tuple key' >> beam.GroupByKey()
        | 'Calculate the mean' >> beam.CombineValues(beam.combiners.MeanCombineFn())
        | 'Convert output to dict' >> beam.Map(transform_to_json)
        )
    bqoutput = (
        pubsub_data
        | beam.Filter(lambda x: isinstance(x,dict))
        | 'Write to bigquery' >> beam.io.WriteToBigQuery(
        outputs_prefix+table_name_veh_info,
        schema=table_veh_info_schema,
        custom_gcs_temp_location=temp_location,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )

from apache_beam.runners.runner import PipelineState
result = p.run()
result.wait_until_finish()
if result.state == PipelineState.DONE:
    print('Success')
else:
    print('Error running beam pipeline')
