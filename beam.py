from typing import Union
import apache_beam as beam
from apache_beam.pvalue import AsList, PCollection
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions

def extract_join_cols(right_element: list[dict], join_key: str, join_cols: Union[list[str], None]):
    if join_cols is not None:
      extract_right_element: list[dict] = [{ k: v for k, v in dic.items() if k in [ *join_cols, join_key ]} for dic in right_element]
      return extract_right_element
    return right_element

def left_join(left_element: dict, right_element: list[dict], join_key: str, join_cols: Union[list[str], None]):
    right_elem: list[dict] = extract_join_cols(right_element, join_key, join_cols)
    for right in right_elem:
      if left_element[join_key] == right[join_key]:
        yield {**left_element, **right}
        return
    
    empty_keys: list[str] = list(filter(lambda x: x != join_key, right_elem[0].keys()))

    # left_element.update({ key: '' for key in empty_keys })
    yield {**left_element, **{ key: '' for key in empty_keys }}

class LeftJoin(beam.PTransform):
  def __init__(self, right_pcoll: PCollection, join_key: str, join_cols: Union[list[str], None]=None):
    self.right_pcoll = right_pcoll
    self.join_key = join_key
    self.join_cols = join_cols
  
  def expand(self, pcoll: PCollection):
    return ( pcoll | beam.FlatMap(
              fn=left_join, 
              right_element=AsList(self.right_pcoll),
              join_key=self.join_key,
              join_cols=self.join_cols
  ))

def run(options: PipelineOptions):
  with beam.Pipeline(options=options) as p:
    # Left PCollection
    left_pcoll = p | "Create Left PCollection" >> beam.Create([
        {'join_key': 1, 'left_value_1': 'a', 'left_value_2': 'b'},
        {'join_key': 2, 'left_value_1': 'c', 'left_value_2': 'd'},
        {'join_key': 3, 'left_value_1': 'e', 'left_value_2': 'f'}
    ])
    
    # Right PCollection
    right_pcoll = p | "Create Right PCollection" >> beam.Create([
        {'join_key': 1, 'right_value': 'A', 'not_join_value': "D"},
        {'join_key': 2, 'right_value': 'B', 'not_join_value': "E"},
        {'join_key': 4, 'right_value': 'C', 'not_join_value': "F"}
    ])

    # Left Join the main and left PCollections
    joined_pcoll = (left_pcoll 
                    | "Left Join" >> LeftJoin(
                        right_pcoll=right_pcoll, 
                        join_key="join_key",
                        join_cols=["right_value"]
                    )
    )
    
    _ = joined_pcoll | "Output" >> beam.Map(print)
  
if __name__=="__main__":
  options = StandardOptions()
  # google_cloud_options = options.view_as(GoogleCloudOptions)
  # google_cloud_options.project = "PROJECT_ID"
  # google_cloud_options.job_name = "JOB_NAME"
  # google_cloud_options.staging_location = 'gs://xxxxxxxxxx'
  # google_cloud_options.temp_location = 'gs://xxxxxxxxxx'
  # options.runner = "DataflowRunner"
  
  options.runner = "DirectRunner"
  run(options)