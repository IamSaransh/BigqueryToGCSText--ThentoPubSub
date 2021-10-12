import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse
import json

parser = argparse.ArgumentParser() 
	
#Path to gcs bucket
parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output file to write results to.')

path_args, pipeline_args = parser.parse_known_args()   

inputs_pattern = path_args.input   
outputs_prefix = path_args.output 


options = PipelineOptions(pipeline_args)
p = beam.Pipeline(options=options)

json_string_output =   (
                          p
                          | 'Read from BQ' >> beam.io.ReadFromBigQuery(
                                query='SELECT * FROM '\
                                 '`project.dataset.table_name`',
                                 use_standard_sql=True)
                          | 'convert to json' >> beam.Map(lambda record: json.dumps(record))
                          | 'Write results' >> beam.io.WriteToText(outputs_prefix)
                      )

p.run()