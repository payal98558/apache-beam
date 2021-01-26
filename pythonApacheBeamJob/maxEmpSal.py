import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from google.cloud import bigquery
#Python library to support command line arguments
import argparse


parser= argparse.ArgumentParser()
#input and output details to be entered during runtime
parser.add_argument('--input',
                      dest='input',
                      required=True,
                      help='Input file to process.'

                    )
parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output file write to output'

                    )

path_args, pipeline_args = parser.parse_known_args()

#Accepting input and output values in code
input_location = path_args.input
output_location = path_args.output

# To make the pipeline generic and run it on any runner
options = PipelineOptions(pipeline_args)


class format_output(beam.DoFn):
    def process (self, record):
        result = [
         "{},{},{}".format(
              record[1][0], record[1][1], record[0])
        ]
        return result



def to_json(csv_str):
    fields = csv_str.split(',')
    json_str = {
        "First_name": fields[0],
        "Last_name": fields[1],
        "Salary": fields[2]
    }
    return json_str


table_schema = 'First_name:STRING,Last_name:STRING, Salary:FLOAT'

with beam.Pipeline(options = options) as p:

    maxEmpSal = (

        p
            | 'Read input file' >> beam.io.ReadFromText(input_location, skip_header_lines=1)
            | 'Split input file' >> beam.Map(lambda record: record.split(','))
            | 'Map records' >> beam.Map(lambda record: (int(record[2]), (record[0], (record[1]))))
            | 'Max Sal' >> beam.CombineGlobally(max)
            | 'format output' >> beam.ParDo(format_output())
            | 'emp record to json' >> beam.Map(to_json)
            | 'write to bigquery' >> beam.io.WriteToBigQuery(
                output_location,
                schema=table_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                )

    )
#BigQuery

#creating dataset
#client = bigquery.Client()

#dataset_id = "potent-bloom-299523.emp_dept_dataset"
#dataset = bigquery.Dataset(dataset_id)
#dataset.location = "US"
#dataset.description = "dataset for emp-dept data"

#dataset_ref = client.create_dataset(dataset, timeout=30)









