import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from google.cloud import bigquery

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
    def process(self,record):
        result = [
         "{},{}".format(
              record[1][0], record[0][0])
        ]
        return result

with beam.Pipeline() as p:
    empDetails = (

        p
            | 'Read from emp' >> beam.io.ReadFromText("emp_data.csv", skip_header_lines=1)
            | 'split emp data' >> beam.Map(lambda record: record.split(','))
            | 'select emp data rows' >> beam.Map(lambda record: (record[3], (int(record[2]))))
            | beam.CombinePerKey(sum)

            )
    deptDetails = (

            p
            | 'Read from dept' >> beam.io.ReadFromText("dept_data.csv", skip_header_lines=1)
            | 'Split dept data' >> beam.Map(lambda record: record.split(','))
            | 'Map values' >> beam.Map(lambda record: ((record[0]), (record[1])))

    )

    results = (
            (deptDetails, empDetails)

            | 'Join emp and dept table' >> beam.CoGroupByKey()
            | 'Map values' >> beam.Map(lambda record: record[1])
            | 'Map Sal and dept name' >> beam.Map(lambda record: [record[1], record[0]])
            | 'max Sal dept' >> beam.CombineGlobally(max)
            | 'Format output' >> beam.ParDo(format_output())
           # | 'Write results' >> beam.io.WriteToText('output')
    )

#BigQuery

#creating dataset
client = bigquery.Client()

dataset_id = "potent-bloom-299523.emp_dept_dataset"
dataset = bigquery.Dataset(dataset_id)
dataset.location = "US"
dataset.description = "dataset for emp-dept data"

dataset_ref = client.create_dataset(dataset, timeout=30)

#creating tables
def to_json(csv_str):
    fields = csv_str.split(',')
    json_str = {
        "Dept_name": fields[0],
        "Salary": fields[1]
    }
    return json_str

table_schema= 'First_name:STRING,Last_name:STRING, Salary:String'

(results
| 'emp record to json' >> beam.Map(to_json)
| 'write to bigquery' >> beam.io.WriteToBigQuery(
output_location,
schema=table_schema,
create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
)

)

