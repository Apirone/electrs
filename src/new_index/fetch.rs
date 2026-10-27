use rayon::prelude::*;

#[cfg(not(feature = "liquid"))]
use bitcoin::consensus::encode::{deserialize, Decodable};
#[cfg(feature = "liquid")]
use elements::encode::{deserialize, Decodable};

use std::collections::HashMap;
use std::fs;
use std::io::Cursor;
use std::path::PathBuf;
use std::thread;

use crate::chain::{Block, BlockHash, BlockSizeCompat};
use crate::daemon::Daemon;
use crate::errors::*;
use crate::util::{spawn_thread, HeaderEntry, SyncChannel};

#[derive(Clone, Copy, Debug)]
pub enum FetchFrom {
    Bitcoind,
    BlkFiles,
}

pub fn start_fetcher(
    from: FetchFrom,
    daemon: &Daemon,
    new_headers: Vec<HeaderEntry>,
) -> Result<Fetcher<Vec<BlockEntry>>> {
    let fetcher = match from {
        FetchFrom::Bitcoind => bitcoind_fetcher,
        FetchFrom::BlkFiles => blkfiles_fetcher,
    };
    fetcher(daemon, new_headers)
}

pub struct BlockEntry {
    pub block: Block,
    pub entry: HeaderEntry,
    pub size: u32,
}

type SizedBlock = (Block, u32);
const DEFAULT_BITCOIND_BLOCK_FETCH_BATCH_SIZE: usize = 100;

pub struct SequentialFetcher<T> {
    fetcher: Box<dyn FnOnce() -> Result<Vec<Vec<T>>>>,
}

impl<T> SequentialFetcher<T> {
    fn from<F: FnOnce() -> Result<Vec<Vec<T>>> + 'static>(pre_func: F) -> Self {
        SequentialFetcher {
            fetcher: Box::new(pre_func),
        }
    }

    pub fn map<FN>(self, mut func: FN) -> Result<()>
    where
        FN: FnMut(Vec<T>),
    {
        for item in (self.fetcher)()? {
            func(item);
        }
        Ok(())
    }
}

pub fn bitcoind_sequential_fetcher(
    daemon: &Daemon,
    new_headers: Vec<HeaderEntry>,
) -> Result<SequentialFetcher<BlockEntry>> {
    let daemon = daemon.reconnect()?;
    Ok(SequentialFetcher::from(move || {
        let mut block_entries = Vec::new();
        for entries in new_headers.chunks(DEFAULT_BITCOIND_BLOCK_FETCH_BATCH_SIZE) {
            block_entries.push(fetch_block_entries(&daemon, entries)?);
        }
        Ok(block_entries)
    }))
}

pub struct Fetcher<T> {
    receiver: crossbeam_channel::Receiver<Result<T>>,
    thread: thread::JoinHandle<()>,
}

impl<T> Fetcher<T> {
    fn from(
        receiver: crossbeam_channel::Receiver<Result<T>>,
        thread: thread::JoinHandle<()>,
    ) -> Self {
        Fetcher { receiver, thread }
    }

    pub fn map<F>(self, mut func: F) -> Result<()>
    where
        F: FnMut(T),
    {
        for item in self.receiver {
            match item {
                Ok(item) => func(item),
                Err(err) => {
                    self.thread.join().expect("fetcher thread panicked");
                    return Err(err);
                }
            }
        }
        self.thread.join().expect("fetcher thread panicked");
        Ok(())
    }
}

fn bitcoind_fetcher(
    daemon: &Daemon,
    new_headers: Vec<HeaderEntry>,
) -> Result<Fetcher<Vec<BlockEntry>>> {
    if let Some(tip) = new_headers.last() {
        debug!("{:?} ({} left to index)", tip, new_headers.len());
    };
    let daemon = daemon.reconnect()?;
    let chan = SyncChannel::new(1);
    let sender = chan.sender();
    Ok(Fetcher::from(
        chan.into_receiver(),
        spawn_thread("bitcoind_fetcher", move || {
            for entries in new_headers.chunks(DEFAULT_BITCOIND_BLOCK_FETCH_BATCH_SIZE) {
                let result = fetch_block_entries(&daemon, entries);
                let failed = result.is_err();
                sender.send(result).expect("failed to send fetched blocks");
                if failed {
                    return;
                }
            }
        }),
    ))
}

fn fetch_block_entries(daemon: &Daemon, entries: &[HeaderEntry]) -> Result<Vec<BlockEntry>> {
    let blockhashes: Vec<BlockHash> = entries.iter().map(|e| *e.hash()).collect();
    let blocks = daemon.getblocks(&blockhashes)?;
    assert_eq!(blocks.len(), entries.len());
    Ok(blocks
        .into_iter()
        .zip(entries)
        .map(|(block, entry)| BlockEntry {
            entry: entry.clone(), // TODO: remove this clone()
            size: block.get_block_size() as u32,
            block,
        })
        .collect())
}

fn blkfiles_fetcher(
    daemon: &Daemon,
    new_headers: Vec<HeaderEntry>,
) -> Result<Fetcher<Vec<BlockEntry>>> {
    let magic = daemon.magic();
    let blk_files = daemon.list_blk_files()?;

    let chan = SyncChannel::new(1);
    let sender = chan.sender();

    let mut entry_map: HashMap<BlockHash, HeaderEntry> =
        new_headers.into_iter().map(|h| (*h.hash(), h)).collect();

    let parser = blkfiles_parser(blkfiles_reader(blk_files), magic);
    Ok(Fetcher::from(
        chan.into_receiver(),
        spawn_thread("blkfiles_fetcher", move || {
            let result = parser.map(|sizedblocks| {
                let block_entries: Vec<BlockEntry> = sizedblocks
                    .into_iter()
                    .filter_map(|(block, size)| {
                        let blockhash = block.block_hash();
                        entry_map
                            .remove(&blockhash)
                            .map(|entry| BlockEntry { block, entry, size })
                            .or_else(|| {
                                trace!("skipping block {}", blockhash);
                                None
                            })
                    })
                    .collect();
                trace!("fetched {} blocks", block_entries.len());
                sender
                    .send(Ok(block_entries))
                    .expect("failed to send blocks entries from blk*.dat files");
            });
            if let Err(err) = result {
                sender
                    .send(Err(err))
                    .expect("failed to send blk*.dat parser error");
            }
            if !entry_map.is_empty() {
                panic!(
                    "failed to index {} blocks from blk*.dat files",
                    entry_map.len()
                )
            }
        }),
    ))
}

fn blkfiles_reader(blk_files: Vec<PathBuf>) -> Fetcher<Vec<u8>> {
    let chan = SyncChannel::new(1);
    let sender = chan.sender();
    let xor_key = blk_files.first().and_then(|p| {
        let xor_file = p
            .parent()
            .expect("blk.dat files must exist in a directory")
            .join("xor.dat");
        if xor_file.exists() {
            Some(fs::read(xor_file).expect("xor.dat exists"))
        } else {
            None
        }
    });

    Fetcher::from(
        chan.into_receiver(),
        spawn_thread("blkfiles_reader", move || {
            for path in blk_files {
                trace!("reading {:?}", path);
                let mut blob = fs::read(&path)
                    .unwrap_or_else(|e| panic!("failed to read {:?}: {:?}", path, e));

                // If the xor.dat exists. Use it to decrypt the block files.
                if let Some(xor_key) = &xor_key {
                    for (&key, byte) in xor_key.iter().cycle().zip(blob.iter_mut()) {
                        *byte ^= key;
                    }
                }

                sender
                    .send(Ok(blob))
                    .unwrap_or_else(|_| panic!("failed to send {:?} contents", path));
            }
        }),
    )
}

fn blkfiles_parser(blobs: Fetcher<Vec<u8>>, magic: u32) -> Fetcher<Vec<SizedBlock>> {
    let chan = SyncChannel::new(1);
    let sender = chan.sender();

    Fetcher::from(
        chan.into_receiver(),
        spawn_thread("blkfiles_parser", move || {
            let result = blobs.map(|blob| {
                trace!("parsing {} bytes", blob.len());
                let blocks = parse_blocks(blob, magic).expect("failed to parse blk*.dat file");
                sender
                    .send(Ok(blocks))
                    .expect("failed to send blocks from blk*.dat file");
            });
            if let Err(err) = result {
                sender
                    .send(Err(err))
                    .expect("failed to send blk*.dat reader error");
            }
        }),
    )
}

fn parse_blocks(blob: Vec<u8>, magic: u32) -> Result<Vec<SizedBlock>> {
    let mut cursor = Cursor::new(&blob);
    let mut slices = vec![];
    let max_pos = blob.len() as u64;

    while cursor.position() < max_pos {
        let offset = cursor.position();
        match u32::consensus_decode(&mut cursor) {
            Ok(value) => {
                if magic != value {
                    cursor.set_position(offset + 1);
                    continue;
                }
            }
            Err(_) => break, // EOF
        };
        let block_size = u32::consensus_decode(&mut cursor).chain_err(|| "no block size")?;
        let start = cursor.position();
        let end = start + block_size as u64;

        // If Core's WriteBlockToDisk ftell fails, only the magic bytes and size will be written
        // and the block body won't be written to the blk*.dat file.
        // Since the first 4 bytes should contain the block's version, we can skip such blocks
        // by peeking the cursor (and skipping previous `magic` and `block_size`).
        match u32::consensus_decode(&mut cursor) {
            Ok(value) => {
                if magic == value {
                    cursor.set_position(start);
                    continue;
                }
            }
            Err(_) => break, // EOF
        }
        slices.push((&blob[start as usize..end as usize], block_size));
        cursor.set_position(end);
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(0) // CPU-bound
        .thread_name(|i| format!("parse-blocks-{}", i))
        .build()
        .unwrap();
    Ok(pool.install(|| {
        slices
            .into_par_iter()
            .map(|(slice, size)| (deserialize(slice).expect("failed to parse Block"), size))
            .collect()
    }))
}
