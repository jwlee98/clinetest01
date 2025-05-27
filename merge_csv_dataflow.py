import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import argparse
import logging
import gzip # For GCS header extraction if needed by storage client, and for WriteToText
from io import BytesIO, TextIOWrapper # For GCS header extraction

# google-cloud-storage는 GCS에서 헤더를 직접 읽어올 때 필요합니다.
# Beam의 GCS IO는 내부적으로 이를 사용합니다.
try:
    from google.cloud import storage
except ImportError:
    logging.error("google-cloud-storage library not found. Please install it using 'pip install google-cloud-storage' if you need to run this script.")
    storage = None

class FilterHeaderStringDoFn(beam.DoFn):
    """
    주어진 헤더 문자열과 정확히 일치하는 라인을 필터링하여 제거합니다.
    """
    def __init__(self, header_string_to_filter):
        # 헤더 문자열이 None이거나 비어있을 경우를 대비하여 초기화
        self.header_string_to_filter = header_string_to_filter.strip() if header_string_to_filter else ""

    def process(self, element):
        # 필터링할 헤더가 실제로 존재하고, 현재 요소와 일치하는 경우에만 건너뜀
        if self.header_string_to_filter and element.strip() == self.header_string_to_filter:
            # logging.debug(f"Skipping header line: {element}") # 디버깅 시 유용
            pass
        else:
            yield element

def extract_header_from_gcs_before_pipeline(gcs_file_pattern: str, project_id: str | None) -> str | None:
    """
    Dataflow 파이프라인 실행 전에 GCS에서 헤더를 추출합니다.
    주어진 GCS 파일 패턴에서 첫 번째 적합한 파일의 첫 번째 줄을 읽습니다.
    파일은 GZIP으로 압축되어 있다고 가정합니다.
    """
    if not storage:
        logging.error("GCS_Ops: google-cloud-storage library is not available. Cannot extract header from GCS.")
        return None
    if not gcs_file_pattern.startswith("gs://"):
        logging.error(f"GCS_Ops: Invalid GCS path for header extraction: {gcs_file_pattern}")
        return None

    logging.info(f"GCS_Ops: Attempting to extract header from GCS pattern: {gcs_file_pattern}")
    try:
        client = storage.Client(project=project_id) # project_id가 None이면 ADC 기본 프로젝트 사용
        
        path_parts = gcs_file_pattern[5:].split("/", 1)
        bucket_name = path_parts[0]
        # 와일드카드(*) 앞부분을 prefix로 사용
        prefix_for_list = path_parts[1].split("*")[0] if len(path_parts) > 1 else ""

        bucket = client.bucket(bucket_name)
        blobs_iterator = bucket.list_blobs(prefix=prefix_for_list)
        
        target_blob = None
        # 패턴과 일치하는 첫 번째 non-empty 파일을 찾기 위한 간단한 로직
        # 예: *.csv.gz 로 끝나는 파일
        file_suffix_to_match = ".csv.gz" # 기본값 또는 패턴에서 더 정확하게 추출 가능
        if "*" in path_parts[1]:
            parts = path_parts[1].split("*")
            if len(parts) > 1 and parts[-1]: 
                file_suffix_to_match = parts[-1] # 예: *.csv.gz -> .csv.gz
            # else: 와일드카드가 복잡하면 이 로직은 단순화된 것임

        for blob_item in blobs_iterator:
            # 1. blob 이름이 의도한 prefix로 시작하는가 (list_blobs가 이미 어느정도 보장)
            # 2. blob 이름이 예상되는 suffix로 끝나는가
            # 3. blob 크기가 0보다 큰가 (빈 파일 제외)
            if blob_item.name.startswith(prefix_for_list) and \
               blob_item.name.endswith(file_suffix_to_match) and \
               blob_item.size > 0:
                target_blob = blob_item
                logging.info(f"GCS_Ops: Found candidate file for header extraction: gs://{bucket_name}/{target_blob.name}")
                break # 첫 번째 적합한 파일 사용
        
        if not target_blob:
            logging.warning(f"GCS_Ops: No suitable GCS files found for header extraction (pattern: '{gcs_file_pattern}', prefix_for_list: '{prefix_for_list}', suffix: '{file_suffix_to_match}').")
            return None

        logging.info(f"GCS_Ops: Extracting header from GCS file: gs://{bucket_name}/{target_blob.name}")
        # 파일의 첫 부분만 다운로드 (예: 처음 4KB)
        gzipped_bytes = target_blob.download_as_bytes(start=0, end=4095) 

        with BytesIO(gzipped_bytes) as byte_stream, \
             gzip.GzipFile(fileobj=byte_stream, mode='rb') as gz_file, \
             TextIOWrapper(gz_file, encoding='utf-8', errors='replace') as text_file: # 인코딩 및 오류 처리 주의
            header_line = text_file.readline().strip()
        
        if not header_line:
            logging.warning(f"GCS_Ops: Extracted an empty header from gs://{bucket_name}/{target_blob.name}")
            return None
        
        logging.info(f"GCS_Ops: Successfully extracted header: \"{header_line}\"")
        return header_line
    except Exception as e:
        logging.error(f"GCS_Ops: Error extracting header from GCS (pattern: {gcs_file_pattern}): {e}", exc_info=True)
        return None

def run(argv=None, save_main_session=True):
    """Defines and runs the GCS CSV merging pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_pattern',
        required=True,
        help='GCS file pattern for input GZIP CSV files (e.g., gs://YOUR_BUCKET/input_folder/*.csv.gz)')
    parser.add_argument(
        '--output_prefix',
        required=True,
        help='GCS path prefix for the output GZIP CSV file (e.g., gs://YOUR_BUCKET/output_folder/merged_output)')
    parser.add_argument(
        '--explicit_header',
        default=None,
        help='(Optional) Explicit header string. If provided, overrides automatic GCS header extraction.')
    # --project 인자를 명시적으로 추가하여 known_args에서 사용할 수 있도록 함
    parser.add_argument(
        '--project',
        default=None, # DirectRunner 사용 시 명시적으로 필요할 수 있음
        help='GCP Project ID. Required by GCS client for header extraction if not inferable from ADC, '
             'and by DataflowRunner.')

    # 파이프라인 옵션 (runner, region 등)은 Beam SDK가 나머지 pipeline_args에서 파싱합니다.
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # pipeline_args에 --project가 여전히 포함되어 DataflowRunner가 사용할 수 있도록 함
    # 만약 known_args.project가 None이 아니고, pipeline_args에 --project가 없다면 추가해줄 수도 있음
    # (하지만 보통 사용자가 --project를 한 번만 제공하므로 pipeline_args에 이미 존재)

    pipeline_options = PipelineOptions(pipeline_args)
    # Dataflow에서 main 세션의 전역 상태를 직렬화하기 위해 필요합니다.
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    
    # GCS 헤더 추출에 사용할 GCP 프로젝트 ID 결정
    # 1. 명시적으로 제공된 --project 인자 사용
    # 2. (DataflowRunner의 경우) pipeline_options에서 가져오기 시도 (GoogleCloudOptions)
    # 3. ADC 환경에서 유추 (storage.Client(project=None) 사용 시)
    gcp_project_for_header_extraction = known_args.project
    if not gcp_project_for_header_extraction:
        try:
            # DataflowRunner의 경우 GoogleCloudOptions에서 project를 가져올 수 있음
            from apache_beam.options.pipeline_options import GoogleCloudOptions
            gcp_project_for_header_extraction = pipeline_options.view_as(GoogleCloudOptions).project
        except AttributeError:
            logging.warning("Could not infer GCP project from pipeline options for header extraction. "
                            "Will rely on GCS client's default project (ADC). "
                            "Consider providing --project argument if issues occur with DirectRunner and GCS.")
    
    if gcp_project_for_header_extraction:
        logging.info(f"Using GCP project '{gcp_project_for_header_extraction}' for GCS header extraction if needed.")
    else:
        logging.info("No GCP project explicitly specified for GCS header extraction; GCS client will use default.")


    # 헤더 결정 로직: 명시적 헤더 > 자동 추출 헤더
    final_header_to_use = known_args.explicit_header
    if not final_header_to_use:
        logging.info("No explicit_header provided. Attempting to auto-extract header from GCS...")
        final_header_to_use = extract_header_from_gcs_before_pipeline(
            known_args.input_pattern,
            gcp_project_for_header_extraction # GCS 클라이언트에 프로젝트 ID 전달
        )
        if not final_header_to_use:
            # 헤더를 결정할 수 없으면 파이프라인을 진행하기 어려움
            error_msg = "Failed to auto-extract header from GCS and no explicit_header was provided. Cannot proceed."
            logging.error(error_msg)
            raise ValueError(error_msg)
    else:
        logging.info(f"Using explicitly provided header: \"{final_header_to_use}\"")

    # --- Apache Beam 파이프라인 정의 ---
    with beam.Pipeline(options=pipeline_options) as p:
        # 1. GCS에서 GZIP 압축된 CSV 파일들을 읽습니다.
        lines = (
            p
            | 'ReadGzippedCsvFilesFromGCS' >> beam.io.ReadFromText(
                known_args.input_pattern,
                compression_type=beam.io.filesystem.CompressionTypes.GZIP
                # skip_header_lines=1 옵션은 모든 파일에 적용되므로,
                # 헤더가 파일마다 다를 수 있거나, 첫 파일만 헤더를 쓰고 싶을 때 부적합.
                # 여기서는 DoFn으로 직접 필터링합니다.
            )
        )

        # 2. 추출/제공된 헤더와 동일한 라인을 필터링하여 제거합니다.
        data_lines_only = (
            lines
            | 'FilterOutHeaderLines' >> beam.ParDo(FilterHeaderStringDoFn(final_header_to_use))
        )

        # 3. 필터링된 데이터 라인들을 단일 GZIP CSV 파일로 GCS에 씁니다.
        #    이때, 결정된 헤더를 파일의 맨 앞에 한 번만 기록합니다.
        data_lines_only | 'WriteSingleGzippedCsvToGCS' >> beam.io.WriteToText(
            known_args.output_prefix,
            file_name_suffix='.csv.gz', # 최종 파일명은 <output_prefix>-00000-of-00001.csv.gz 형태
            compression_type=beam.io.filesystem.CompressionTypes.GZIP,
            num_shards=1,  # 단일 출력 파일을 생성하기 위한 핵심 설정
            header=final_header_to_use # 파일 상단에 이 헤더를 씁니다.
        )
    
    logging.info(f"Dataflow pipeline job submission initiated (or local run started).")
    logging.info(f"Header to be used: \"{final_header_to_use}\"")
    logging.info(f"Output will be written with prefix: {known_args.output_prefix}")

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    
    # --- 로컬 테스트 (DirectRunner) 예시 ---
    # 로컬에서 GCS 파일로 테스트하려면 GCP 인증(ADC)이 설정되어 있어야 합니다.
    # (예: gcloud auth application-default login)
    # 그리고 `pip install "apache-beam[gcp]"` 로 의존성을 설치해야 합니다.
    #
    # python ./merge_csv_dataflow.py \
    #    --input_pattern="gs://YOUR_BUCKET/path/to/sample_input/*.csv.gz" \
    #    --output_prefix="/tmp/merged_output_local_run" \
    #    --runner=DirectRunner \
    #    --project=YOUR_GCP_PROJECT_ID \
    #    # --explicit_header="ID,Name,Value" # 자동 추출 대신 수동 지정 시

    # --- Dataflow 서비스 실행 예시 ---
    # python ./merge_csv_dataflow.py \
    #    --input_pattern="gs://YOUR_PRODUCTION_BUCKET/input_data_path/*.csv.gz" \
    #    --output_prefix="gs://YOUR_PRODUCTION_BUCKET/output_data_path/merged_all_data" \
    #    --runner=DataflowRunner \
    #    --project=YOUR_GCP_PROJECT_ID \
    #    --region=YOUR_GCP_REGION \
    #    --temp_location="gs://YOUR_TEMP_BUCKET/dataflow_temp/" \
    #    --staging_location="gs://YOUR_STAGING_BUCKET/dataflow_staging/" \
    #    --service_account_email=YOUR_DATAFLOW_WORKER_SERVICE_ACCOUNT_EMAIL \
    #    --max_num_workers=50 \
    #    # (데이터 양과 원하는 처리 시간에 따라 조절. 예: 10 ~ 200)
    #    --machine_type="n2d-standard-4" \
    #    # (CPU와 메모리가 더 많은 머신 타입. GZIP 압축/해제는 CPU 사용량이 많음)
    #    # --disk_size_gb=100 \
    #    # (작업자당 디스크 크기. 대용량 셔플이나 데이터 유출 대비)
    #    # --experiments=shuffle_mode=service \
    #    # (Dataflow Shuffle Service 사용, 배치 작업에 권장)
    #    # --experiments=use_runner_v2 \
    #    # (Runner V2 사용, 일부 워크로드에서 성능 향상 가능성. 충분한 테스트 필요)

    run()
