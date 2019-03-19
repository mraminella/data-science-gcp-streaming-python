from apache_beam.options.pipeline_options import PipelineOptions
from tensorflow.python.lib.io import file_io
from datetime import timedelta
from datetime import datetime
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

import apache_beam as beam
import tensorflow as tf
import numpy as np
import argparse
import logging
import numpy
import time
import json
import sys
import os
import csv


class print_class(beam.DoFn) :
    def process(self,element):
        print(str(element))

class select_arrived(beam.DoFn) :
    def process(self,element):
        if len(element) == 0:
            return
        row = str(element).split(',')
        event = row[-2]
        if event == "arrived":
            key = row[12]
            value = row[22]
            yield ( key, value )
        
class average_delay(beam.DoFn) :
    def process(self,element):
        ( key, values ) = element
        values = np.array(values).astype(np.float)
        mean = np.mean(values)
        yield [( "dest" , key), mean ]
        
class in_latest_slice(beam.DoFn) :
    def __init__(self, averagingFrequency):
        self.averagingFrequency = averagingFrequency
    def process(self,element,window=beam.DoFn.WindowParam,timestamp=beam.DoFn.TimestampParam):
        endOfWindow = window.end
        flightTimeStamp = timestamp
        msecs = float(endOfWindow) - float(flightTimeStamp)
        if msecs < self.averagingFrequency:
            yield element
            
class add_dep_delay(beam.DoFn) :
    def process(self,element,avgDepDelay): # SIDE INPUT
        if len(element) == 0:
            return
        row = str(element).split(',')
        depKey = row[8]
        try:
            depDelay = float(avgDepDelay[depKey])
        except:
            depDelay = 0.0
        row.append(depDelay)
        yield row

class avg_dep_delay(beam.DoFn):
    def process(self,element):
        arrKey = element[12] # DEST
        yield [ ("dest" , arrKey) , element ]
        
class add_arr_delay(beam.DoFn) :
    def process(self,element):
        content = element[1]
        if (len(content["airportFlights"]) > 0):
            if (len(content["avgArrDelay"]) == 0):
                content["avgArrDelay"].append(0.0)
                
            for flight in content["airportFlights"] :
                flight.append(content["avgArrDelay"][0])
                yield flight
        return
        
class create_batch(beam.DoFn) :
    def process(self,element):
        num_batches = 2
        if(len(element[25]) == 0):
            element[25] = 0.00
        if ( float(element[23]) > 0 ) or  ( float(element[25]) > 0 ): # cancelled or diverted
            element[33] = "ignored"
        key = str(element[33]) + " " + str(hash(str(element)) % num_batches)
        yield ( key , element )

        
def predict(project,model_name,model_version,flights):
    credentials = GoogleCredentials.get_application_default()
    api = discovery.build('ml', 'v1', cache_discovery=False, credentials=credentials,
              discoveryServiceUrl='https://storage.googleapis.com/cloud-ml/discovery/ml_v1_discovery.json')
    parent = 'projects/{}/models/{}/versions/{}'.format(project, model_name, model_version)
    
    instances = []
    for flight in flights:
        instance = {	
           'dep_delay': float(flight[15]),
           'taxiout': float(flight[16]),
           'distance': float(flight[26]),
           'avg_dep_delay': float(flight[35]),
           'avg_arr_delay': float(flight[36]),
           'carrier': flight[3],
           'dep_lat': float(flight[27]),
           'dep_lon': float(flight[28]),
           'arr_lat': float(flight[30]),
           'arr_lon': float(flight[31]),
           'origin': flight[8],
           'dest': flight[12]
        }
        instances.append(instance)

    request_data = {'instances': instances }
    response = api.projects().predict(body=request_data, name=parent).execute()
    if response.has_key(u'predictions'):
        result = []
        for prediction in response[u'predictions']:
            onTimeProbability = prediction[u'probabilities'][1]
            result.append(onTimeProbability)
        return result
            
class inference(beam.DoFn) :
    def __init__(self, project, model_name, model_version):
        self.project = project
        self.model_name = model_name
        self.model_version = model_version
    def process(self, element):
        if element[0].startswith(str("ignored")):
            for flight in element[1]:
                flight.append(-1)
                yield(flight)
        if element[0].startswith(str("arrived")):
            for flight in element[1]:
                if(float(flight[22]) < 15.0):
                    flight.append(1)
                    yield(flight)
                else:
                    flight.append(0)
                    yield(flight)
        if element[0].startswith(str("wheelsoff")):
            result = predict(self.project,self.model_name,self.model_version,element[1])
            if(result != None):
                for i in range(len(element[1])):
                    element[1][i].append(result[i])
                    yield(element[1][i])

def create_row(fields):
    header = 'FL_DATE,UNIQUE_CARRIER,AIRLINE_ID,CARRIER,FL_NUM,ORIGIN_AIRPORT_ID,ORIGIN_AIRPORT_SEQ_ID,ORIGIN_CITY_MARKET_ID,ORIGIN,DEST_AIRPORT_ID,DEST_AIRPORT_SEQ_ID,DEST_CITY_MARKET_ID,DEST,CRS_DEP_TIME,DEP_TIME,DEP_DELAY,TAXI_OUT,WHEELS_OFF,WHEELS_ON,TAXI_IN,CRS_ARR_TIME,ARR_TIME,ARR_DELAY,CANCELLED,CANCELLATION_CODE,DIVERTED,DISTANCE,DEP_AIRPORT_LAT,DEP_AIRPORT_LON,DEP_AIRPORT_TZOFFSET,ARR_AIRPORT_LAT,ARR_AIRPORT_LON,ARR_AIRPORT_TZOFFSET,EVENT,NOTIFY_TIME,ONTIME'.split(',') 
    featdict = {}
    for name, value in zip(header, fields):
        if value != '':
            featdict[name] = value 
    return featdict
                            
def run(project,model_name,model_version,dataset,averagingInterval,averagingFrequency):
    cloud = "false"
    if cloud.lower() == "true":
        argv = [
           '--project='+opt.project,
           '--job_name=telemar-day7-streaming-'+str(time.time()).replace(".",""),
           '--network=datalab-network',
           #'--num_workers=2',
           '--autoscaling_algorithm=THROUGHPUT_BASED',
           '--max_num_workers=750',
           '--runner=DataflowRunner',
           '--streaming'
       ]
    elif cloud.lower() == "false":
        argv = [
           '--project=injenia-ricerca',
           '--runner=DirectRunner',
           '--streaming'
       ]
    else:
        raise Exception("Cloud param must be either True or False")
   
    schema = 'FL_DATE:date,UNIQUE_CARRIER:string,AIRLINE_ID:string,CARRIER:string,FL_NUM:string,ORIGIN_AIRPORT_ID:string,ORIGIN_AIRPORT_SEQ_ID:integer,ORIGIN_CITY_MARKET_ID:string,ORIGIN:string,DEST_AIRPORT_ID:string,DEST_AIRPORT_SEQ_ID:integer,DEST_CITY_MARKET_ID:string,DEST:string,CRS_DEP_TIME:timestamp,DEP_TIME:timestamp,DEP_DELAY:float,TAXI_OUT:float,WHEELS_OFF:timestamp,WHEELS_ON:timestamp,TAXI_IN:float,CRS_ARR_TIME:timestamp,ARR_TIME:timestamp,ARR_DELAY:float,CANCELLED:string,CANCELLATION_CODE:string,DIVERTED:string,DISTANCE:float,DEP_AIRPORT_LAT:float,DEP_AIRPORT_LON:float,DEP_AIRPORT_TZOFFSET:float,ARR_AIRPORT_LAT:float,ARR_AIRPORT_LON:float,ARR_AIRPORT_TZOFFSET:float,EVENT:string,NOTIFY_TIME:timestamp,EVENT_DATA:string,ONTIME:float'
    events_output = '{}:{}.sim_ontime'.format(project, dataset)
    print("writing to: " + events_output)
    
    # Create and set PipelineOptions.
    options         = PipelineOptions()
    topics          = { "wheelsoff" : "projects/injenia-ricerca/topics/wheelsoff", "arrived" : "projects/injenia-ricerca/topics/arrived" }
    pipeline        = beam.Pipeline(argv=argv)
    
    arrived         = (pipeline | 'pubsub:read2s' >> beam.io.ReadFromPubSub(topic=topics["arrived"],timestamp_attribute="EventTimeStamp"))
    # timestamp_attribute è meglio lasciarlo perdere perchè tende a ignorare gli eventi vecchi
    
    wheelsoff       = (pipeline | 'pubsub:read1' >> beam.io.ReadFromPubSub(topic=topics["wheelsoff"],timestamp_attribute="EventTimeStamp"))
    
    flattened       = ((wheelsoff, arrived) | 'pubsub:flattened' >> beam.Flatten())
    
    avgDepDelay     = (pipeline 
                       | 'flights:read' >> beam.io.ReadFromText('gs://raminella-dev-mlengine/flights/chapter8/output/delays.csv')
                       | 'flights:fields' >> beam.Map(lambda line: next(csv.reader([line])))
                       | 'flights:delay' >> beam.Map(lambda fields: (fields[0]) )
                       )
    
    hourlyFlights   = (flattened 
                       | 'flattened:windowing' >> 
                            beam.WindowInto(beam.window.SlidingWindows(size=averagingInterval, period=averagingFrequency))
                       )
    
    avgArrDelay     = (hourlyFlights 
                       | 'flights:selectArrived' >> beam.ParDo(select_arrived())
                       | 'flights:groupByKey'  >> beam.GroupByKey()
                       | 'flights:calculateAverage' >> beam.ParDo(average_delay())
                       )
    
    
    airportFlights  = (hourlyFlights
                       | 'delayInfo:InLatestSlice' >> beam.ParDo(in_latest_slice(averagingFrequency))
                       | 'delay:addDepDelay' >> beam.ParDo(add_dep_delay(),beam.pvalue.AsDict(avgDepDelay)) # QUESTO E' UN SIDE INPUT!!!!
                       | 'delay:avgDepDelay' >> beam.ParDo(avg_dep_delay())
                       )
    
    result          =  ({"airportFlights" : airportFlights, "avgArrDelay" : avgArrDelay} 
                       | 'result:CoGroupByKey' >> beam.CoGroupByKey()
                       | 'result:AddArrDelay' >> beam.ParDo(add_arr_delay())
                       )
    
    flightsOnTime   = (result
                       | 'writeFlights:CreateBatch' >> beam.ParDo(create_batch())
                       | 'writeFlights:GroupByKey' >> beam.GroupByKey()
                       | 'writeFlights:Inference' >> beam.ParDo(inference(project,model_name,model_version))
                       )
    
    (flightsOnTime
                       | 'writeFlights:MapToFields' >> beam.Map(lambda fields: create_row(fields))
                       | 'writeFlights:WriteToBQ' >>  beam.io.WriteToBigQuery(events_output,schema=schema,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND, 
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )
   
    pipeline.run().wait_until_finish()
    
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    ############# CLOUD PARAMS #############
    parser.add_argument('--project',
                       default="injenia-ricerca",
                       help='GCP project')
    ############# IO PARAMS #############
    parser.add_argument('--model',
                       default="emme_flights",
                       help='modello previsioni MLengine flights')
    parser.add_argument('--version',
                       default='v1',
                       help='versione modello MLengine')
    parser.add_argument('--dataset',
                       help='dataset bigquery',
                       default='flights') 
    ############# OTHER PARAMS #############
    parser.add_argument('--timestep',
                       type=int,
                       default=60,
                       help='dimensione in minuti del timestep, deve coincidere con il campionamento dell\'aggregazione')
    parser.add_argument('--window_size',
                       type=int,
                       default=5,
                       help='dimensione in timestep della finestra di osservazione')

    logging.getLogger().setLevel(logging.INFO)
    args = vars(parser.parse_args())
    run(project=args['project'], model_name=args['model'],model_version=args['version'],dataset=args['dataset'], averagingInterval=args['timestep'],averagingFrequency=args['window_size'])