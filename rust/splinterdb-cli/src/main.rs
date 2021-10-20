use clap::Parser;

/// SplinterDB command line tool
#[derive(Parser)]
#[clap(name = "splinterdb-cli", version = "0.1")]
struct Opts {
    #[clap(short, long)]
    file: String,

    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Parser)]
enum SubCommand {
    InitDB(InitDB),
    Insert(Insert),
    Delete(Delete),
    Get(Get),
    List(List),
    Perf(Perf),
}

/// Insert a key-value pair into an existing database
#[derive(Parser, Debug)]
struct Insert {
    /// Key to insert
    #[clap(short, long)]
    pub key: String,

    /// Value to insert
    #[clap(short, long)]
    pub value: String,
}

/// Delete a key and its value from an existing database
#[derive(Parser, Debug)]
struct Delete {
    /// Key to delete
    #[clap(short, long)]
    pub key: String,
}

/// Get the value for a key from an existing database
#[derive(Parser, Debug)]
struct Get {
    /// Key to lookup
    #[clap(short, long)]
    pub key: String,
}

/// List all keys and values in an existing database
#[derive(Parser, Debug)]
struct List {}

/// Initialize a new database file, using the kvstore_basic interface
#[derive(Parser, Debug)]
struct InitDB {
    /// Size of in-memory cache, in MB
    #[clap(short, long, default_value = "30")]
    pub cache_mb: u16,

    /// Size of file to use on disk, in MB
    #[clap(short, long, default_value = "100")]
    pub disk_mb: u16,

    /// Maximum length of keys, in bytes
    #[clap(short, long, default_value = "16")]
    pub key_size: u8,

    /// Maximum length of values, in bytes
    #[clap(short, long, default_value = "100")]
    pub value_size: u8,
}

const MB: usize = 1024 * 1024;

type CLIResult<T> = Result<T, Box<dyn ::std::error::Error>>;

use std::fs::File;
use std::path::Path;

fn get_metadata_path(db_path: &str) -> String {
    format!("{}.meta", db_path)
}

fn meta_save(db_path: &str, db_config: &splinterdb_rs::DBConfig) -> CLIResult<()> {
    let meta_file = File::create(Path::new(&get_metadata_path(db_path)))?;
    ::serde_json::ser::to_writer(meta_file, db_config)?;
    Ok(())
}

fn meta_load(db_path: &str) -> CLIResult<splinterdb_rs::DBConfig> {
    let mut meta_file = File::open(Path::new(&get_metadata_path(db_path)))?;
    let mut bytes = Vec::new();
    use std::io::Read;
    meta_file.read_to_end(&mut bytes)?;
    let db_config = serde_json::from_slice(&bytes)?;
    Ok(db_config)
}

impl InitDB {
    fn run(&self, opts: &Opts) -> CLIResult<()> {
        let db_config = splinterdb_rs::DBConfig {
            cache_size_bytes: (self.cache_mb as usize) * MB,
            disk_size_bytes: (self.disk_mb as usize) * MB,
            max_key_size: self.key_size,
            max_value_size: self.value_size,
        };
        meta_save(&opts.file, &db_config)?;

        let db = splinterdb_rs::db_create(&opts.file, &db_config)?;
        drop(db);
        Ok(())
    }
}

impl Get {
    fn run(&self, opts: &Opts) -> CLIResult<()> {
        let db_config = meta_load(&opts.file)?;
        let db = splinterdb_rs::db_open(&opts.file, &db_config)?;
        let res = db.lookup(self.key.as_bytes())?;
        match res {
            splinterdb_rs::LookupResult::NotFound => Err("key not found".into()),
            splinterdb_rs::LookupResult::FoundTruncated(_) => {
                Err("value truncated: this is a bug".into())
            }
            splinterdb_rs::LookupResult::Found(v) => {
                let v = std::str::from_utf8(&v)?;
                println!("{}", v);
                Ok(())
            }
        }
    }
}

impl Insert {
    fn run(&self, opts: &Opts) -> CLIResult<()> {
        let db_config = meta_load(&opts.file)?;
        let db = splinterdb_rs::db_open(&opts.file, &db_config)?;
        let key = self.key.as_bytes();
        let val = self.value.as_bytes();
        db.insert(key, val)?;
        Ok(())
    }
}

impl Delete {
    fn run(&self, opts: &Opts) -> CLIResult<()> {
        let db_config = meta_load(&opts.file)?;
        let db = splinterdb_rs::db_open(&opts.file, &db_config)?;
        let key = self.key.as_bytes();
        db.delete(key)?;
        Ok(())
    }
}

impl List {
    fn run(&self, opts: &Opts) -> CLIResult<()> {
        let db_config = meta_load(&opts.file)?;
        let db = splinterdb_rs::db_open(&opts.file, &db_config)?;
        let mut iter = db.range(None)?;
        loop {
            match iter.next() {
                Ok(Some(&splinterdb_rs::IteratorResult { key, value })) => {
                    let key = std::str::from_utf8(key)?;
                    let value = std::str::from_utf8(value)?;
                    println!("\t{} : {}", key, value)
                }
                Ok(None) => {
                    println!("<end of list>");
                    break;
                }
                Err(e) => {
                    println!("got error: {:?}", e);
                    break;
                }
            }
        }
        Ok(())
    }
}

use rand::{Rng, SeedableRng};
use rand_pcg::Pcg64;

use crossbeam_utils::thread;
use std::time::Instant;

/// Test performance.  Will overwrite the target file with random data.
#[derive(Parser, Debug)]
pub struct Perf {
    /// Number of insert threads
    #[clap(short, long, default_value = "1")]
    threads: u8,

    /// Number of writes to do on each thread
    #[clap(short, long, default_value = "10000")]
    writes_per_thread: u32,

    /// Random seed
    #[clap(long, default_value = "0")]
    seed: u64,

    /// Size of in-memory cache, in MB
    #[clap(long, default_value = "400")]
    cache_mb: u16,

    /// Size of file to use on disk, in MB
    #[clap(long, default_value = "9000")]
    disk_mb: u32,
}

impl Perf {
    const KEY_SIZE: u8 = 20;
    const VALUE_SIZE: u8 = 116;
    const REPORT_PERIOD: u32 = 500000;

    pub fn run(&self, file: String) -> CLIResult<()> {
        let db_config = splinterdb_rs::DBConfig {
            cache_size_bytes: self.cache_mb as usize * MB,
            disk_size_bytes: self.disk_mb as usize * MB,
            max_key_size: Perf::KEY_SIZE,
            max_value_size: Perf::VALUE_SIZE,
        };
        let path = file;

        let db = splinterdb_rs::db_create(&path, &db_config).unwrap();

        let work_start_time = Instant::now();
        thread::scope(|s| {
            for i in 0..self.threads {
                let db = &db;
                let i = i;
                let num_writes = self.writes_per_thread;

                s.spawn(move |_| {
                    db.register_thread();
                    let mut rng = Pcg64::seed_from_u64(i as u64);

                    for count in 0..num_writes {
                        let mut key = [0u8; Perf::KEY_SIZE as usize];
                        let mut value = [0u8; Perf::VALUE_SIZE as usize];
                        Perf::rand_fill_buffer(&mut rng, &mut key);
                        Perf::rand_fill_buffer(&mut rng, &mut value);
                        db.insert(&key, &value).unwrap();

                        if count % Perf::REPORT_PERIOD == 0 {
                            eprint!(".");
                        }
                    }
                    db.deregister_thread();
                });
            }
        })
        .unwrap();
        drop(db); // flush all caches to disk

        let work_complete_time = Instant::now();
        let total_work_time = (work_complete_time - work_start_time).as_secs_f32();

        let total_writes = self.threads as u64 * self.writes_per_thread as u64;
        let mb_written =
            total_writes * (Perf::KEY_SIZE as u64 + Perf::VALUE_SIZE as u64) / MB as u64;

        eprintln!(
            "\n{:>8} {:>12} {:>12} {:>8} {:>8} {:>15}",
            "threads", "inserts", "MB_inserted", "seconds", "bw_MBps", "inserts/sec"
        );
        println!(
            "{:>8} {:>12} {:>12} {:>8.2} {:>8.2} {:>15.2}",
            self.threads,
            total_writes,
            mb_written,
            total_work_time,
            mb_written as f32 / total_work_time,
            total_writes as f32 / total_work_time,
        );

        Ok(())
    }

    fn rand_fill_buffer(rng: &mut Pcg64, to_fill: &mut [u8]) {
        for x in to_fill.iter_mut() {
            *x = rng.gen();
        }
    }
}

fn main() -> CLIResult<()> {
    let opts: Opts = Opts::parse();

    match opts.subcmd {
        SubCommand::InitDB(ref init_db) => init_db.run(&opts),
        SubCommand::Insert(ref insert) => insert.run(&opts),
        SubCommand::Delete(ref delete) => delete.run(&opts),
        SubCommand::Get(ref get) => get.run(&opts),
        SubCommand::List(ref list) => list.run(&opts),
        SubCommand::Perf(ref perf) => perf.run(opts.file),
    }
}
