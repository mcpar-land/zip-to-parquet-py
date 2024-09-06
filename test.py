from zip_to_parquet import convert_zip_to_parquet
import polars as pl
from glob import glob
import time

tests = [
    ("/home/mcp/Downloads/ballots_98d1ad69adbe6a19/*.zip", "/home/mcp/Downloads/test_1.parquet"),
    ("/home/mcp/Downloads/CVRExport-7-15-2024 11-06-49 AM.zip", "/home/mcp/Downloads/test_2.parquet"),
]

values = []
for input_path, output_path in tests:
    def dbg_print(body, hash):
        start = time.time()
        convert_zip_to_parquet(glob(input_path), output_path, include_body=body, include_hash=hash)
        end = time.time()
        elapsed_time = end - start
        n_rows = pl.scan_parquet(output_path).select(pl.len()).collect().item(0, 0)
        print("done in", elapsed_time, "seconds")
        return {
            "input_path": input_path,
            "output_path": output_path,
            "body": body,
            "hash": hash,
            "n_rows": n_rows,
            "elapsed_time": elapsed_time
        }
    values.append(dbg_print(False, False))
    values.append(dbg_print(False, True))
    values.append(dbg_print(True, False))
    values.append(dbg_print(True, True))

print(pl.DataFrame(values))
