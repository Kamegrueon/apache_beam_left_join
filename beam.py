from typing import Union

# Apache Beamのライブラリをインポート
import apache_beam as beam
from apache_beam.pvalue import AsList, PCollection
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# 右側のテーブルから指定されたカラムを取得する関数
def extract_join_cols(right_element: list[dict], join_key: str, join_cols: Union[list[str], None]):
    if join_cols is not None:
      # リスト内包表記を用いて指定されたカラムだけを取得し、新しいリストを作成
      extract_right_element: list[dict] = [{ k: v for k, v in dic.items() if k in [ *join_cols, join_key ]} for dic in right_element]
      return extract_right_element
    # 指定されたカラムがなければそのまま返す
    return right_element

# 左側のテーブルと右側のテーブルを結合する関数
def left_join(left_element: dict, right_element: list[dict], join_key: str, join_cols: Union[list[str], None]):
    # 指定されたカラムを抽出
    right_elem: list[dict] = extract_join_cols(right_element, join_key, join_cols)
    for right in right_elem:
      # join_keyが一致する場合は結合し、yieldで返す
      if left_element[join_key] == right[join_key]:
        yield {**left_element, **right}
        return
      
    # join_keyが一致しない場合、空のデータを追加して結合する
    empty_keys: list[str] = list(filter(lambda x: x != join_key, right_elem[0].keys()))
    yield {**left_element, **{ key: '' for key in empty_keys }}

# Apache BeamのPTransformを継承して、LeftJoinという新しいPTransformを作成する
class LeftJoin(beam.PTransform):
  def __init__(self, right_pcoll: PCollection, join_key: str, join_cols: Union[list[str], None]=None):
    self.right_pcoll = right_pcoll
    self.join_key = join_key
    self.join_cols = join_cols
  
   # PTransformを拡張するためのメソッド
  def expand(self, pcoll: PCollection):
    return ( pcoll | beam.FlatMap(
              fn=left_join, # 左テーブルの各行と右テーブルのデータを結合するためにleft_join関数を使用する
              right_element=AsList(self.right_pcoll), # 右テーブルのデータをAsListでリスト化する
              join_key=self.join_key, # 結合に使用するキー
              join_cols=self.join_cols # 右テーブルから結合したいカラムのリスト、Noneの場合は全てのカラムを使用する
  ))
    
# Apache Beamのパイプラインを実行する関数
def run(options: PipelineOptions):
  with beam.Pipeline(options=options) as p:
    # 左側のテーブルを作成する
    left_pcoll = p | "Create Left PCollection" >> beam.Create([
        {'join_key': 1, 'left_value_1': 'a', 'left_value_2': 'b'},
        {'join_key': 2, 'left_value_1': 'c', 'left_value_2': 'd'},
        {'join_key': 3, 'left_value_1': 'e', 'left_value_2': 'f'}
    ])
    
    # 右側のテーブルを作成する
    right_pcoll = p | "Create Right PCollection" >> beam.Create([
        {'join_key': 1, 'right_value': 'A', 'not_join_value': "D"},
        {'join_key': 2, 'right_value': 'B', 'not_join_value': "E"},
        {'join_key': 4, 'right_value': 'C', 'not_join_value': "F"}
    ])

    # 左外部結合を実行
    joined_pcoll = (left_pcoll 
                    | "Left Join" >> LeftJoin(
                        right_pcoll=right_pcoll, 
                        join_key="join_key",
                        join_cols=["right_value"]
                    )
    )
    
    # 結果を出力
    _ = joined_pcoll | "Output" >> beam.Map(print)
  
if __name__=="__main__":
  # パイプラインオプションを設定して実行
  options = StandardOptions()  
  options.runner = "DirectRunner"
  run(options)