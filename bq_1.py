"""An example workflow that demonstrates filters and other features.
  - Reading and writing data from BigQuery.
  - Manipulating BigQuery rows (as Python dicts) in memory.
  - Global aggregates.
  - Filtering PCollections using both user-specified parameters
    as well as global aggregates computed during pipeline execution.
"""

# pytype: skip-file

import argparse
import logging

import apache_beam as beam
from apache_beam.pvalue import AsSingleton


def run(argv=None):
  """Constructs and runs the example filtering pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      help='BigQuery table to read from.',
      default='testproject-321809:KamalDataset.KamalTable')
  parser.add_argument(
      '--output', default='testproject-321809:KamalDataset.KamalTable2', help='BigQuery table to write to.')
  #parser.add_argument(
   #   '--month_filter', default=7, help='Numeric value of month to filter on.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_args.extend([
      # CHANGE 2/6: (OPTIONAL) Change this to DataflowRunner to
      # run your pipeline on the Google Cloud Dataflow Service.
      '--runner=DataflowRunner',
      # CHANGE 3/6: (OPTIONAL) Your project ID is required in order to
      # run your pipeline on the Google Cloud Dataflow Service.
      '--project=testproject-321809',
      # CHANGE 4/6: (OPTIONAL) The Google Cloud region (e.g. us-central1)
      # is required in order to run your pipeline on the Google Cloud
      # Dataflow Service.
      '--region=us-central1',
      # CHANGE 5/6: Your Google Cloud Storage path is required for staging local
      # files.
      '--staging_location=gs://kamal1980/Temp',
      # CHANGE 6/6: Your Google Cloud Storage path is required for temporary
      # files.
      '--temp_location=gs://kamal1980/Temp',
      '--job_name=bq1',
	  '--num_workers=3',
  ])


  with beam.Pipeline(argv=pipeline_args) as p:

    input_data = p | beam.io.ReadFromBigQuery(table=known_args.input)

    # pylint: disable=expression-not-assigned
    (
        input_data
		| 'FilterOnFlag' >> beam.Filter(lambda row: row['flag'] == 'Y')
        | 'SaveToBQ' >> beam.io.WriteToBigQuery(
            known_args.output,
            schema='userid:STRING,amount:STRING,flag:STRING',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()