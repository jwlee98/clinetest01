import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import argparse
import logging
import re
import gzip
from io import BytesIO, TextIOWrapper

# google-cloud-storage 라이브러리가 필요합니다.
# pip install google-cloud-storage
try:
    from google.cloud import storage
except ImportError:
    logging.error("google-cloud-storage library not found. Please install it using 'pip install google-cloud-storage'")
    # Dataflow 작업자 환경에서는 setup.py 또는 requirements.txt를 통해 설치해야 할 수 있습니다.
    storage = None


class FilterHeader(beam.DoFn):
    """
    주어진 헤더 문자열과 일치하는 라인을 필터링하여 제거합니다.
    """
    def __init__(self, header_to_filter):
        self.header_to_filter = header_to_filter.strip() if header_to_filter else ""

    def process(self, element):
        if self.header_to_filter and element.strip() == self.header_to_filter:
            pass  # 헤더 라인이므로 건너뜁니다.
        else:
            yield element # 데이터 라인이거나 헤더가 없는 경우


def extract_header_from_gcs(gcs_file_pattern: str, project: str | None = None) -> str | None:
    """
    주어진 GCS 파일 패턴에서 첫 번째 파일의 첫 번째 줄(헤더)을 읽어 반환합니다.
    파일은 GZIP으로 압축되어 있다고 가정합니다.
    'project' 인자는 GCS 클라이언트 초기화 시 특정 프로젝트를 지정해야 할 때 사용됩니다 (선택 사항).
    """
    if not storage:
        logging.error("Google Cloud Storage client is not available. Cannot extract header.")
        return None

    try:
        client = storage.Client(project=project) if project else storage.Client()
        
        if not gcs_file_pattern.startswith("gs://"):
            raise ValueError("Invalid GCS path. Must start with gs://")
        
        path_parts = gcs_file_pattern[5:].split("/", 1)
        bucket_name = path_parts[0]
        prefix_pattern = path_parts[1] if len(path_parts) > 1 else ""
        
        blob_name_prefix = prefix_pattern.split("*")[0]

        bucket = client.bucket(bucket_name)
        blobs_iterator = bucket.list_blobs(prefix=blob_name_prefix)
        
        target_blob = None
        file_suffix_to_match = ".csv.gz" 
        if "*" in prefix_pattern:
            parts = prefix_pattern.split("*")
            if len(parts) > 1 and parts[-1]: 
                 file_suffix_to_match = parts[-1]
            elif len(parts) == 1 and not parts[0]: 
                 file_suffix_to_match = "" 
        
        for blob_item in blobs_iterator:
            if (not file_suffix_to_match or blob_item.name.endswith(file_suffix_to_match)) and blob_item.size > 0:
                if blob_item.name.startswith(blob_name_prefix): # Ensure it's within the intended directory structure
                    target_blob = blob_item
                    break 
        
        if not target_blob:
            logging.warning(f"No suitable non-empty files found for header extraction with GCS pattern '{gcs_file_pattern}' (using prefix '{blob_name_prefix}' and suffix '{file_suffix_to_match}').")
            return None

        logging.info(f"Attempting to extract header from: gs://{bucket_name}/{target_blob.name}")

        range_end = 4095 
        gzipped_bytes = target_blob.download_as_bytes(start=0, end=range_end) 

        with BytesIO(gzipped_bytes) as byte_stream:
            with gzip.GzipFile(fileobj=byte_stream, mode='rb') as gz_file:
                with TextIOWrapper(gz_file, encoding='utf-8', errors='replace') as text_file: 
                    header_line = text_file.readline().strip()
                    if not header_line:
                        logging.warning(f"Extracted an empty header from gs://{bucket_name}/{target_blob.name}")
                        return None
                    logging.info(f"Successfully extracted header: \"{header_line}\"")
                    return header_line

    except Exception as e:
        logging.error(f"Error extracting header from GCS (pattern: {gcs_file_pattern}): {e}", exc_info=True)
        return None


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_pattern', dest='input_pattern', required=True,
        help='GCS file pattern for input GZIP CSV files (e.g., gs://your-bucket/input/*.csv.gz)')
    parser.add_argument(
        '--output_prefix', dest='output_prefix', required=True,
        help='GCS path prefix for output GZIP CSV file (e.g., gs://your-bucket/output/merged_data)')
    parser.add_argument(
        '--header', dest='header', default=None, 
        help='(Optional) Header string to write. If provided, overrides auto-extraction. '
             'If not provided, attempts to extract from the first input file.')
    parser.add_argument(
        '--gcs_project_for_header', dest='gcs_project_for_header', default=None,
        help='(Optional) GCP project ID to use for GCS client when extracting header. '
             'Defaults to the environment\'s default project.')


    known_args, pipeline_args = parser.parse_known_args(argv)

    final_header = known_args.header
    if not final_header:
        logging.info("No explicit header provided. Attempting to extract header from input files...")
        final_header = extract_header_from_gcs(known_args.input_pattern, project=known_args.gcs_project_for_header)
        if not final_header:
            logging.error("Failed to extract header automatically and no explicit header provided. "
                          "Ensure input files exist, are accessible, and the GCS client has permissions. "
                          "You might need to specify --gcs_project_for_header if running locally or with specific credentials.")
            raise ValueError("Header could not be determined. Provide --header or check GCS access for auto-extraction.")
    else:
        logging.info(f"Using explicitly provided header: \"{final_header}\"")

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        lines = (
            p
            | 'ReadInputFiles' >> beam.io.ReadFromText(
                known_args.input_pattern,
                compression_type=beam.io.filesystem.CompressionTypes.GZIP
            )
        )

        data_lines = (
            lines
            | 'FilterAllHeaders' >> beam.ParDo(FilterHeader(final_header))
        )

        data_lines | 'WriteOutputFile' >> beam.io.WriteToText(
            known_args.output_prefix,
            file_name_suffix='.csv.gz',
            compression_type=beam.io.filesystem.CompressionTypes.GZIP,
            num_shards=1,
            header=final_header
        )

    logging.info(f"Dataflow pipeline job submission initiated. Output will be at {known_args.output_prefix}-XXXXX-of-00001.csv.gz (or similar).")
    logging.info(f"The header used for the output file is: \"{final_header}\"")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
