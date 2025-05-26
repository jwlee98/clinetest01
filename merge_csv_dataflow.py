import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.pvalue import AsSingleton
import argparse
import logging

# google-cloud-storage는 Beam의 GCS IO에 의해 내부적으로 사용되므로,
# 명시적 import는 헤더 추출을 위해 GCS 클라이언트를 직접 사용할 때만 필요합니다.
# 이 버전에서는 Beam 내부 기능을 사용하므로 직접적인 GCS 클라이언트 사용은 없습니다.

class FilterHeaderDoFn(beam.DoFn):
    """
    사이드 입력으로 받은 헤더와 일치하는 라인을 필터링합니다.
    """
    def process(self, element, header_as_side_input):
        # header_as_side_input은 단일 문자열로 예상됨
        # element는 현재 처리 중인 라인
        if element.strip() == header_as_side_input.strip():
            pass  # 헤더 라인이므로 건너뜁니다.
        else:
            yield element # 데이터 라인입니다.

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_pattern',
        dest='input_pattern',
        required=True,
        help='GCS file pattern for input GZIP CSV files (e.g., gs://your-bucket/input/*.csv.gz)')
    parser.add_argument(
        '--output_prefix',
        dest='output_prefix',
        required=True,
        help='GCS path prefix for output GZIP CSV file (e.g., gs://your-bucket/output/merged_data)')
    # --header 인자는 이제 사용하지 않음 (자동 추출)
    # 필요하다면, 수동 헤더 지정을 위한 옵션을 다시 추가할 수 있습니다.

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    runner_name = pipeline_options.view_as(PipelineOptions).runner
    if runner_name == 'DirectRunner' or not runner_name:
        logging.info("Running with DirectRunner. Ensure GCS paths are accessible and GCP ADC is set.")
        if not known_args.input_pattern.startswith("gs://") or not known_args.output_prefix.startswith("gs://"):
            logging.warning("DirectRunner is used, but non-GCS paths might be provided. This script is optimized for GCS.")


    with beam.Pipeline(options=pipeline_options) as p:
        # 1. 모든 GCS 파일에서 모든 라인을 읽습니다.
        lines = (
            p
            | 'ReadInputFiles' >> beam.io.ReadFromText(
                known_args.input_pattern,
                compression_type=beam.io.filesystem.CompressionTypes.GZIP
            )
        )

        # 2. 샘플 헤더 추출:
        #    모든 파일의 첫 번째 라인이 헤더이고 동일하다고 가정합니다.
        #    따라서 전체 PCollection에서 하나의 라인을 샘플링하면 그것이 헤더가 됩니다.
        #    주의: 입력 파일 중 빈 파일이 없어야 하고, 모든 파일이 헤더로 시작해야 합니다.
        #    AsSingleton을 사용하여 PCollection<str>을 일반 str 값으로 변환 (사이드 입력용)
        extracted_header_as_pcoll_str = (
            lines
            | 'SampleOneLineForHeader' >> beam.combiners.Sample.FixedSizeGlobally(1) # 결과는 list[str] 형태
            | 'GetFirstSampledLine' >> beam.Map(lambda lst: lst[0] if lst else "") # list에서 첫번째 str 추출, 없으면 빈 문자열
        )
        # extracted_header_as_pcoll_str는 이제 단일 문자열을 가진 PCollection입니다.

        # 3. 데이터 필터링:
        #    추출된 헤더(사이드 입력)를 사용하여 모든 라인에서 헤더를 제거합니다.
        data_lines = (
            lines
            | 'FilterHeaderLines' >> beam.ParDo(
                FilterHeaderDoFn(), 
                header_as_side_input=AsSingleton(extracted_header_as_pcoll_str) # 사이드 입력으로 전달
            )
        )
        
        # 4. 최종 파일 작성:
        #    필터링된 데이터 라인과 추출된 헤더를 사용하여 단일 GZIP CSV 파일로 GCS에 씁니다.
        #    WriteToText의 header 매개변수는 PCollection이 아닌 일반 문자열을 기대합니다.
        #    따라서, extracted_header_as_pcoll_str을 직접 사용할 수 없고,
        #    만약 WriteToText의 header 인자에 PCollection을 전달해야 한다면,
        #    파이프라인 구성 시점에 실제 값을 알아야 하므로 이 방식은 적합하지 않습니다.
        #
        #    대안: WriteToText는 header 인자로 일반 문자열을 받습니다.
        #    파이프라인 구성 시점에 헤더 값을 알아야 하므로,
        #    이전처럼 파이프라인 실행 전에 헤더를 읽거나, 사용자가 명시적으로 제공해야 합니다.
        #
        #    여기서는 "모든 파일의 첫 줄이 헤더"라는 가정을 최대한 활용하여,
        #    WriteToText의 header 인자에는 extracted_header_as_pcoll_str의 "결과값"을 사용해야 합니다.
        #    하지만 Beam 파이프라인은 지연 실행되므로, 구성 시점에는 그 값을 알 수 없습니다.
        #
        #    가장 간단한 해결책 (이전 방식으로 회귀):
        #    - 파이프라인 실행 전에 GCS에서 헤더를 읽어오는 로직을 다시 사용합니다.
        #    - 또는, 사용자가 --header로 명시적으로 제공하도록 합니다.
        #
        #    여기서는 Beam의 철학을 따르면서 문제를 해결하기 위해,
        #    헤더를 데이터의 일부로 간주하고, 최종적으로 헤더를 한 번만 추가하는 다른 방법을 고려합니다.
        #    1. 모든 헤더를 제거한 data_lines를 만듭니다.
        #    2. extracted_header_as_pcoll_str (단일 헤더 라인 PCollection)을 만듭니다.
        #    3. 이 두 PCollection을 Flatten으로 합치고, 헤더가 먼저 오도록 정렬하거나,
        #       WriteToText가 PCollection을 순서대로 쓴다는 점을 이용합니다. (주의 필요)
        #
        #    더 간단하고 안정적인 방법:
        #    WriteToText의 header 매개변수를 사용하기 위해, 파이프라인 실행 전에 헤더를 결정합니다.
        #    (사용자 피드백 "GCS 경로 처리만 남겨주고 header를 좀 더 효율적으로 처리"에 따라
        #     파이프라인 외부 I/O를 최소화하려 했으나, WriteToText의 제약으로 인해 한계가 있음)
        #
        #    **수정된 접근 방식:**
        #    파이프라인 외부에서 GCS 클라이언트로 헤더를 읽는 로직을 다시 도입합니다.
        #    이것이 WriteToText의 header 파라미터와 가장 잘 통합됩니다.
        #    "효율적"이라는 의미를 "Beam 파이프라인 내에서 모든 것을 처리"로 해석했으나,
        #    실용성과 WriteToText의 API를 고려하면 외부 추출이 나을 수 있습니다.
        #    사용자의 "효율적"이 "실행 시간"을 의미한다면, 외부 추출은 무시할 만한 오버헤드입니다.

        # --- 이전의 GCS 헤더 추출 로직 (WriteToText와 호환성을 위해 다시 도입) ---
        # 이 로직은 파이프라인 실행 전에 수행되어야 합니다.
        # run() 함수 초반으로 이동시키는 것이 적절합니다.
        # 여기서는 설명을 위해 이 위치에 두지만, 실제로는 run() 함수 상단에서 호출됩니다.

        # (아래 코드는 실제로는 run 함수 상단에 위치해야 함)
        # final_header_value = extract_header_from_gcs_outside_pipeline(known_args.input_pattern, pipeline_options.view_as(PipelineOptions).project)
        # if not final_header_value:
        #     raise ValueError("Could not determine header for WriteToText.")
        #
        # data_lines | 'WriteOutputFile' >> beam.io.WriteToText(...) (header=final_header_value)

        # === 최종 결정: 사용자 피드백을 존중하여, 파이프라인 외부 GCS 헤더 추출 로직을 사용 ===
        # (이 코드는 실제로는 run 함수 상단에 위치해야 함. 아래 run 함수에서 해당 로직을 호출)
        # final_header_value 는 run 함수 초반에 결정됩니다.
        # 여기서는 data_lines를 WriteToText로 바로 연결하고, header는 run 함수에서 결정된 값을 사용합니다.

        # (FilterHeaderDoFn과 SampleOneLineForHeader는 이 접근 방식에서는 사용되지 않음)
        # (대신, 이전 버전의 FilterHeader(header_string) DoFn을 사용)

        # === 스크립트를 이전 버전(외부 GCS 헤더 추출)으로 되돌리고 GCS 전용으로 만듭니다. ===
        # (이 파일의 내용은 이전 버전의 GCS 전용 스크립트로 대체되어야 합니다.)
        # (사용자의 피드백을 반영하여 코드를 다시 작성합니다.)

        # 이 블록은 이전 버전의 코드로 대체될 것입니다.
        # 현재 이 상태로는 실행되지 않습니다.
        # 다음 메시지에서 수정된 전체 스크립트를 제공하겠습니다.
        # 여기서는 개념 설명 후 중단합니다.
        pass # 임시


    # logging.info(f"Pipeline execution initiated. Output will be at (or prefixed by): {known_args.output_prefix}")
    # logging.info(f"The header used for the output file is: \"{final_header_value}\"")


# GCS에서 헤더를 읽는 함수 (파이프라인 외부 실행용)
def extract_header_from_gcs_for_pipeline_run(gcs_file_pattern: str, project_id: str | None) -> str | None:
    from google.cloud import storage # 이 함수 내에서만 import
    
    if not gcs_file_pattern.startswith("gs://"):
        logging.error("Invalid GCS path for header extraction.")
        return None
    try:
        client = storage.Client(project=project_id)
        path_parts = gcs_file_pattern[5:].split("/", 1)
        bucket_name = path_parts[0]
        prefix_pattern = path_parts[1] if len(path_parts) > 1 else ""
        blob_name_prefix = prefix_pattern.split("*")[0]
        
        bucket = client.bucket(bucket_name)
        blobs_iterator = bucket.list_blobs(prefix=blob_name_prefix)
        
        target_blob = None
        file_suffix_to_match = ".csv.gz" # 기본값 또는 패턴에서 추출
        if "*" in prefix_pattern:
            parts = prefix_pattern.split("*")
            if len(parts) > 1 and parts[-1]: file_suffix_to_match = parts[-1]
            elif len(parts) == 1 and not parts[0]: file_suffix_to_match = ""

        for blob_item in blobs_iterator:
            if (not file_suffix_to_match or blob_item.name.endswith(file_suffix_to_match)) and blob_item.size > 0:
                if blob_item.name.startswith(blob_name_prefix):
                    target_blob = blob_item
                    break
        
        if not target_blob:
            logging.warning(f"GCS: No suitable files found for header extraction (pattern: '{gcs_file_pattern}').")
            return None

        logging.info(f"GCS: Extracting header from: gs://{bucket_name}/{target_blob.name}")
        gzipped_bytes = target_blob.download_as_bytes(start=0, end=4095) # 첫 4KB
        import gzip
        from io import BytesIO, TextIOWrapper
        with BytesIO(gzipped_bytes) as byte_stream, \
             gzip.GzipFile(fileobj=byte_stream, mode='rb') as gz_file, \
             TextIOWrapper(gz_file, encoding='utf-8', errors='replace') as text_file:
            header_line = text_file.readline().strip()
        if not header_line:
            logging.warning(f"GCS: Extracted empty header from gs://{bucket_name}/{target_blob.name}")
            return None
        logging.info(f"GCS: Extracted header: \"{header_line}\"")
        return header_line
    except Exception as e:
        logging.error(f"GCS: Error extracting header (pattern: {gcs_file_pattern}): {e}", exc_info=True)
        return None

# 이전 버전의 FilterHeader DoFn (문자열 헤더를 직접 받음)
class FilterHeaderStringDoFn(beam.DoFn):
    def __init__(self, header_string_to_filter):
        self.header_string_to_filter = header_string_to_filter.strip()

    def process(self, element):
        if element.strip() == self.header_string_to_filter:
            pass
        else:
            yield element

# run 함수 재정의 (GCS 전용, 외부 헤더 추출 사용)
def run_gcs_optimized(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_pattern', required=True, help='GCS input pattern (e.g., gs://bucket/input/*.csv.gz)')
    parser.add_argument('--output_prefix', required=True, help='GCS output prefix (e.g., gs://bucket/output/merged)')
    # --header 인자는 사용자가 원하면 추가 가능 (자동 추출 덮어쓰기용)
    parser.add_argument('--explicit_header', default=None, help='(Optional) Explicit header string to use.')


    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    
    gcp_project_id = pipeline_options.view_as(PipelineOptions).project
    if not gcp_project_id:
        # 로컬에서 DirectRunner로 GCS 접근 시 --project 필요할 수 있음
        logging.warning("GCP project ID not found in pipeline options. Header extraction from GCS might fail if not set via ADC default.")


    # 헤더 결정 (명시적 > 자동 추출)
    final_header_value = known_args.explicit_header
    if not final_header_value:
        logging.info("Attempting to auto-extract header from GCS...")
        final_header_value = extract_header_from_gcs_for_pipeline_run(known_args.input_pattern, gcp_project_id)
        if not final_header_value:
            raise ValueError("Failed to auto-extract header from GCS and no explicit_header provided.")
    else:
        logging.info(f"Using explicit_header: \"{final_header_value}\"")


    with beam.Pipeline(options=pipeline_options) as p:
        lines = (
            p
            | 'ReadGCSInputFiles' >> beam.io.ReadFromText(
                known_args.input_pattern,
                compression_type=beam.io.filesystem.CompressionTypes.GZIP
            )
        )

        data_lines = (
            lines
            | 'FilterHeaderLines' >> beam.ParDo(FilterHeaderStringDoFn(final_header_value))
        )

        data_lines | 'WriteGCSOutputFile' >> beam.io.WriteToText(
            known_args.output_prefix,
            file_name_suffix='.csv.gz',
            compression_type=beam.io.filesystem.CompressionTypes.GZIP,
            num_shards=1,
            header=final_header_value # 추출/제공된 헤더 사용
        )
    
    logging.info(f"Pipeline for GCS initiated. Header: \"{final_header_value}\". Output: {known_args.output_prefix}-...")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    # run() 대신 run_gcs_optimized() 호출
    # 예시:
    # python ./merge_csv_dataflow.py \
    #    --input_pattern="gs://YOUR_BUCKET/input_folder/*.csv.gz" \
    #    --output_prefix="gs://YOUR_BUCKET/output_folder/merged_gcs_data" \
    #    --runner=DataflowRunner \
    #    --project=YOUR_GCP_PROJECT_ID \
    #    --region=YOUR_GCP_REGION \
    #    --temp_location="gs://YOUR_BUCKET/temp_folder/"
    #    # --explicit_header="col1,col2,col3" # 필요시
    run_gcs_optimized()

