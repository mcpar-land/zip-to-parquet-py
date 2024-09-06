use std::{
    error::Error,
    io::{BufReader, BufWriter, Read},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, SyncSender},
        Arc,
    },
    thread,
};

use arrow_array::{ArrayRef, BinaryArray, RecordBatch, StringArray};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use pyo3::{exceptions::PyOSError, prelude::*};
use sha2::{Digest, Sha256};
use threadpool::ThreadPool;
use wax::{Glob, Pattern};
use zip::ZipArchive;

/// A Python module implemented in Rust.
#[pymodule]
fn zip_to_parquet(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(convert_zip_to_parquet, m)?)?;
    Ok(())
}

const DEFAULT_ROW_GROUP_SIZE: usize = 1_000_000;

#[pyfunction]
#[pyo3(signature=(
    zip_paths,
    parquet_path,
    glob = None,
    include_body = true,
    include_hash = true,
    row_group_size = DEFAULT_ROW_GROUP_SIZE,
))]
fn convert_zip_to_parquet(
    zip_paths: Vec<String>,
    parquet_path: String,
    glob: Option<String>,
    include_body: bool,
    include_hash: bool,
    row_group_size: usize,
) -> PyResult<()> {
    let terminated = Arc::new(AtomicBool::new(false));
    let n_cores = thread::available_parallelism()?.get();
    let pool = ThreadPool::new(n_cores);

    let rx = {
        let (tx, rx) = mpsc::sync_channel::<Result<UnzippedFile, PyErr>>(row_group_size);
        for zip_path in &zip_paths {
            let glob = glob.clone();
            let zip_path = zip_path.clone();
            let tx = tx.clone();
            let terminated = terminated.clone();
            pool.execute(move || {
                if let Err(err) = handle_read_zip(
                    &zip_path,
                    glob,
                    include_body,
                    include_hash,
                    tx.clone(),
                    terminated,
                ) {
                    tx.send(Err(err)).unwrap();
                }
            });
        }
        rx
    };

    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .set_max_row_group_size(row_group_size)
        .build();
    let schema = RecordBatch::try_from_iter(vec![
        (
            "name",
            Arc::new(StringArray::from(Vec::<String>::new())) as ArrayRef,
        ),
        (
            "source",
            Arc::new(StringArray::from(Vec::<String>::new())) as ArrayRef,
        ),
        (
            "body",
            Arc::new(BinaryArray::from(Vec::<Option<&[u8]>>::new())) as ArrayRef,
        ),
        (
            "hash",
            Arc::new(StringArray::from(Vec::<String>::new())) as ArrayRef,
        ),
    ])
    .map_err(to_py_err)?
    .schema();

    let mut writer = ArrowWriter::try_new(
        BufWriter::new(std::fs::File::create(&parquet_path)?),
        schema,
        Some(props),
    )
    .map_err(to_py_err)?;

    let mut file_names = Vec::<String>::with_capacity(row_group_size);
    let mut file_sources = Vec::<String>::with_capacity(row_group_size);
    let mut file_contents = Vec::<Option<Vec<u8>>>::with_capacity(row_group_size);
    let mut file_hashes = Vec::<Option<String>>::with_capacity(row_group_size);

    for input in rx {
        match input {
            Ok(input) => {
                file_names.push(input.name);
                file_sources.push(input.source);
                file_contents.push(input.body);
                file_hashes.push(input.hash);
                if file_names.len() >= row_group_size {
                    writer = write_row_group(
                        writer,
                        &mut file_names,
                        &mut file_sources,
                        &mut file_contents,
                        &mut file_hashes,
                        &terminated,
                    )?;
                }
            }
            Err(err) => {
                terminated.swap(true, Ordering::Relaxed);
                return Err(err);
            }
        }
    }
    if file_names.len() > 0 {
        writer = write_row_group(
            writer,
            &mut file_names,
            &mut file_sources,
            &mut file_contents,
            &mut file_hashes,
            &terminated,
        )?;
    }

    writer.close().map_err(to_py_err)?;

    Ok(())
}

pub struct UnzippedFile {
    pub name: String,
    pub source: String,
    pub body: Option<Vec<u8>>,
    pub hash: Option<String>,
}

fn handle_read_zip(
    path: &str,
    glob: Option<String>,
    include_body: bool,
    include_hash: bool,
    tx: SyncSender<Result<UnzippedFile, PyErr>>,
    terminated: Arc<AtomicBool>,
) -> PyResult<()> {
    let glob = glob.as_ref().map(|glob| Glob::new(&glob).unwrap());
    let mut zip =
        ZipArchive::new(BufReader::new(std::fs::File::open(&path)?)).map_err(to_py_err)?;
    for i in 0..zip.len() {
        if terminated.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }
        let file = zip.by_index(i).map_err(to_py_err)?;
        let is_in_glob = glob
            .as_ref()
            .map(|glob| glob.is_match(file.name()))
            .unwrap_or(true);
        if !is_in_glob {
            continue;
        }
        let name = file.name().to_string();
        let (body, hash) = if !include_body && !include_hash {
            (None, None)
        } else {
            let file_body = file.bytes().collect::<Result<Vec<u8>, std::io::Error>>()?;
            let hash = if include_hash {
                let mut hasher = Sha256::new();
                hasher.update(&file_body);
                let hash = hasher
                    .finalize()
                    .iter()
                    .map(|v| format!("{:x}", v))
                    .collect::<Vec<String>>()
                    .join("");
                let hash_str = format!("{:x?}", hash);
                Some(hash_str)
            } else {
                None
            };
            let body = if include_body { Some(file_body) } else { None };
            (body, hash)
        };
        tx.send(Ok(UnzippedFile {
            name,
            source: path.to_string(),
            body,
            hash,
        }))
        .map_err(to_py_err)?;
    }
    Ok(())
}

fn write_row_group(
    mut writer: ArrowWriter<BufWriter<std::fs::File>>,
    file_names: &mut Vec<String>,
    file_sources: &mut Vec<String>,
    file_contents: &mut Vec<Option<Vec<u8>>>,
    file_hashes: &mut Vec<Option<String>>,
    terminated: &Arc<AtomicBool>,
) -> Result<ArrowWriter<BufWriter<std::fs::File>>, PyErr> {
    let file_names_column = StringArray::from(file_names.drain(0..).collect::<Vec<String>>());
    let file_sources_column = StringArray::from(file_sources.drain(0..).collect::<Vec<String>>());
    let file_contents_column = BinaryArray::from(
        file_contents
            .iter()
            .map(|v| v.as_ref().map(|v| v.as_ref()))
            .collect::<Vec<Option<&[u8]>>>(),
    );
    let file_hashes_column =
        StringArray::from(file_hashes.drain(0..).collect::<Vec<Option<String>>>());
    let batch = RecordBatch::try_from_iter(vec![
        ("name", Arc::new(file_names_column) as ArrayRef),
        ("source", Arc::new(file_sources_column) as ArrayRef),
        ("body", Arc::new(file_contents_column) as ArrayRef),
        ("hash", Arc::new(file_hashes_column) as ArrayRef),
    ])
    .map_err(to_py_err)?;
    writer.write(&batch).map_err(to_py_err)?;
    if terminated.load(std::sync::atomic::Ordering::Relaxed) {
        return Ok(writer);
    }
    file_names.clear();
    file_sources.clear();
    file_contents.clear();
    file_hashes.clear();
    Ok(writer)
}

fn to_py_err<E: Error>(err: E) -> PyErr {
    PyOSError::new_err(err.to_string())
}
