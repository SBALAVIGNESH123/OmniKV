//! SSTable reader, writer, and iterator for sorted on-disk key-value storage.

use std::fs::File;
use std::io::{BufWriter, Write};

use crate::record::{OmniError, OmniRecord};

pub struct SSTableReader<'a> {
    data: &'a [u8],
}
impl<'a> SSTableReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    fn get_block_for_key(&self, target: &[u8]) -> Option<usize> {
        if self.data.len() < 16 {
            return Some(0);
        }
        let magic = &self.data[self.data.len() - 8..];
        if magic != b"OMNIV2**" {
            return Some(0);
        }

        let index_offset = u64::from_le_bytes(
            self.data[self.data.len() - 16..self.data.len() - 8]
                .try_into()
                .unwrap(),
        ) as usize;
        if index_offset >= self.data.len() {
            return Some(0);
        }

        let index_data = &self.data[index_offset..self.data.len() - 16];
        if index_data.len() < 8 {
            return Some(0);
        }
        let num_entries = u64::from_le_bytes(index_data[0..8].try_into().unwrap()) as usize;

        let mut entries = Vec::with_capacity(num_entries);
        let mut curr = 8;
        for _ in 0..num_entries {
            if curr + 2 > index_data.len() {
                break;
            }
            let key_len =
                u16::from_le_bytes(index_data[curr..curr + 2].try_into().unwrap()) as usize;
            curr += 2;
            if curr + key_len + 8 > index_data.len() {
                break;
            }
            let key = &index_data[curr..curr + key_len];
            curr += key_len;
            let offset =
                u64::from_le_bytes(index_data[curr..curr + 8].try_into().unwrap()) as usize;
            curr += 8;
            entries.push((key, offset));
        }

        let idx = entries.partition_point(|&(k, _)| k <= target);
        if idx == 0 {
            Some(0)
        } else {
            Some(entries[idx - 1].1)
        }
    }

    pub fn find(&self, target_key: &[u8], read_seq: u64) -> Option<(u64, u64, u32, u64)> {
        let mut offset = self.get_block_for_key(target_key).unwrap_or(0);
        let mut best = None;
        let mut best_seq = 0;

        while offset < self.data.len() {
            if self.data.len() >= 16 && &self.data[self.data.len() - 8..] == b"OMNIV2**" {
                let index_offset = u64::from_le_bytes(
                    self.data[self.data.len() - 16..self.data.len() - 8]
                        .try_into()
                        .unwrap(),
                ) as usize;
                if offset >= index_offset {
                    break;
                }
            }

            if let Some((rec, len)) = OmniRecord::decode(&self.data[offset..]) {
                offset += len;
                if rec.key.as_slice() > target_key {
                    break;
                }
                if rec.key.as_slice() == target_key && rec.is_valid() && rec.seq <= read_seq {
                    if best.is_none() || rec.seq > best_seq {
                        best_seq = rec.seq;
                        best = Some((rec.offset, rec.length, rec.payload_crc32, rec.expiry));
                    }
                }
            } else {
                break;
            }
        }
        best
    }

    pub fn iter_from(&self, start_key: &[u8]) -> SSTableIterator<'a> {
        let offset = self.get_block_for_key(start_key).unwrap_or(0);
        SSTableIterator {
            data: self.data,
            offset,
        }
    }
}

pub struct SSTableIterator<'a> {
    data: &'a [u8],
    offset: usize,
}
impl<'a> Iterator for SSTableIterator<'a> {
    type Item = OmniRecord;
    fn next(&mut self) -> Option<Self::Item> {
        if self.data.len() >= 16 && &self.data[self.data.len() - 8..] == b"OMNIV2**" {
            let index_offset = u64::from_le_bytes(
                self.data[self.data.len() - 16..self.data.len() - 8]
                    .try_into()
                    .unwrap(),
            ) as usize;
            if self.offset >= index_offset {
                return None;
            }
        }
        if let Some((rec, len)) = OmniRecord::decode(&self.data[self.offset..]) {
            self.offset += len;
            Some(rec)
        } else {
            None
        }
    }
}

pub struct SSTableWriter<'a> {
    writer: BufWriter<&'a File>,
    offset: usize,
    index: Vec<(Vec<u8>, u64)>,
    current_block_start_key: Option<Vec<u8>>,
    block_size: usize,
}

impl<'a> SSTableWriter<'a> {
    pub fn new(file: &'a File) -> Self {
        Self {
            writer: BufWriter::new(file),
            offset: 0,
            index: Vec::new(),
            current_block_start_key: None,
            block_size: 4096,
        }
    }

    pub fn append(&mut self, record: &OmniRecord) -> Result<(), OmniError> {
        let bytes = record.encode();

        if self.current_block_start_key.is_none() {
            self.current_block_start_key = Some(record.key.clone());
            self.index.push((record.key.clone(), self.offset as u64));
        }

        self.writer.write_all(&bytes)?;
        self.offset += bytes.len();

        if let Some(_) = &self.current_block_start_key {
            if self.offset as u64 - self.index.last().unwrap().1 >= self.block_size as u64 {
                self.current_block_start_key = None;
            }
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<(), OmniError> {
        let index_offset = self.offset as u64;
        self.writer
            .write_all(&(self.index.len() as u64).to_le_bytes())?;
        for (k, off) in &self.index {
            self.writer.write_all(&(k.len() as u16).to_le_bytes())?;
            self.writer.write_all(k)?;
            self.writer.write_all(&off.to_le_bytes())?;
        }
        self.writer.write_all(&index_offset.to_le_bytes())?;
        self.writer.write_all(b"OMNIV2**")?;
        self.writer.flush()?;
        Ok(())
    }
}
