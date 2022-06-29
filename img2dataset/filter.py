"""Parquet filter for searching high resolution images"""

import os
import fsspec
import pyarrow.parquet as pq
import pyarrow.csv as csv_pq

def filter_high_resolution(folder_path, output_folder, size):
  filters = [('WIDTH', '>=', size), ('HEIGHT', '>=', size)]
  columns_to_read = ["URL", "TEXT", "WIDTH", "HEIGHT", "similarity", "punsafe", "pwatermark"]

  fs, url_path = fsspec.core.url_to_fs(folder_path)
  if fs.isdir(folder_path):
    input_files = sorted(fs.glob(url_path + "/*.parquet"))
    print(f"There are {len(input_files)} parquet files to handle with.")
    if len(input_files) == 0:
        raise Exception(f"No parquet files found at path {url_path}")

  total_count = 0
  write_options = csv_pq.WriteOptions(include_header=False)
  for i, input_file in enumerate(input_files):
    with fs.open(input_file, mode="rb") as file:
      df = pq.read_table(file, columns=columns_to_read, filters=filters)
      if df.num_rows > 0:
        total_count += df.num_rows
        print(f"{df.num_rows} samples found, total {total_count} samples.")
        name = os.path.splitext(os.path.basename(file.name))[0]
        csv_pq.write_csv(df, f"{output_folder}/{name}.csv", write_options=write_options)
  
  del df

filter_high_resolution(
  "/data/download/laion2B-en",
  "/data/scripts/img2dataset-main/hq",
  1024
)
