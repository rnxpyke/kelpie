use crate::Chunk;

#[derive(thiserror::Error, Debug)]
pub enum SetChunkError {
    #[error("Driver error")]
    Driver(#[from] Box<dyn std::error::Error>),
}

#[derive(thiserror::Error, Debug)]
pub enum GetChunkError {
    #[error("Driver error")]
    Driver(#[from] Box<dyn std::error::Error>),
}

pub trait KelpieChunkStore {
    fn get_chunk(
        &self,
        series_key: i64,
        start: i64,
        stop: i64,
    ) -> Result<Option<(ChunkMeta, Chunk)>, GetChunkError>;
    fn set_chunk(
        &mut self,
        series_key: i64,
        start: i64,
        stop: i64,
        chunk: &Chunk,
    ) -> Result<(), SetChunkError>;
}

pub struct SqliteChunkStore {
    db: sqlite::Connection,
}

impl SqliteChunkStore {
    fn migrate(db: &mut sqlite::Connection) -> Result<(), sqlite::Error> {
        db.execute("CREATE TABLE IF NOT EXISTS chunks (series INTEGER, start INTEGER, stop INTEGER, chunk BLOB)")?;
        Ok(())
    }

    pub fn new_memory() -> Result<Self, sqlite::Error> {
        let mut db = sqlite::open(":memory:")?;
        Self::migrate(&mut db)?;
        Ok(Self { db })
    }

    pub fn new_path<T: AsRef<std::path::Path>>(path: T) -> Result<Self, sqlite::Error> {
        let mut db = sqlite::open(path)?;
        Self::migrate(&mut db)?;
        Ok(Self { db })
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ChunkMeta {
    pub series_key: i64,
    pub start: i64,
    pub stop: i64,
}

impl KelpieChunkStore for SqliteChunkStore {
    fn get_chunk(
        &self,
        series_key: i64,
        start: i64,
        stop: i64,
    ) -> Result<Option<(ChunkMeta, Chunk)>, GetChunkError> {
        fn driver(e: sqlite::Error) -> GetChunkError {
            GetChunkError::Driver(e.into())
        }
        let mut statement = self.db.prepare("SELECT start, stop, chunk from chunks WHERE series == ? AND start <= ? AND stop >= ? ORDER BY start DESC LIMIT 1").map_err(driver)?;

        statement.bind((1, series_key)).map_err(driver)?;
        statement.bind((2, start)).map_err(driver)?;
        statement.bind((3, stop)).map_err(driver)?;

        let mut res = None;
        if let sqlite::State::Row = statement.next().map_err(driver)? {
            let res_start: i64 = statement.read::<i64, _>("start").map_err(driver)?;
            let res_stop: i64 = statement.read("stop").map_err(driver)?;
            dbg!(res_start, res_stop);
            let res_chunk: Vec<u8> = statement.read("chunk").map_err(driver)?;
            let meta = ChunkMeta {
                series_key,
                start: res_start,
                stop: res_stop,
            };
            let chunk = Chunk {
                compressed_data: res_chunk,
            };
            res = Some((meta, chunk));
        }

        statement.reset().map_err(driver)?;
        Ok(res)
    }

    fn set_chunk(
        &mut self,
        series_key: i64,
        start: i64,
        stop: i64,
        chunk: &Chunk,
    ) -> Result<(), SetChunkError> {
        fn driver(e: sqlite::Error) -> SetChunkError {
            SetChunkError::Driver(e.into())
        }
        let mut statement = self
            .db
            .prepare("INSERT INTO chunks VALUES (?, ?, ?, ?)")
            .map_err(driver)?;
        statement.bind((1, series_key)).map_err(driver)?;
        statement.bind((2, start)).map_err(driver)?;
        statement.bind((3, stop)).map_err(driver)?;
        statement
            .bind((4, chunk.compressed_data.as_slice()))
            .map_err(driver)?;
        loop {
            let state = statement.next().map_err(driver)?;
            match state {
                sqlite::State::Row => {}
                sqlite::State::Done => break,
            }
        }
        statement.reset().map_err(driver)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{Chunk, KelpieChunkStore};

    #[test]
    fn should_create_sqlite_chunk_store() -> Result<(), Box<dyn std::error::Error>> {
        let _store = super::SqliteChunkStore::new_memory()?;
        Ok(())
    }

    #[test]
    fn should_store_chunk() -> Result<(), Box<dyn std::error::Error>> {
        let mut store = super::SqliteChunkStore::new_memory()?;
        let chunk = Chunk {
            compressed_data: vec![],
        };
        store.set_chunk(0, 10, 100, &chunk)?;
        Ok(())
    }

    #[test]
    fn should_not_find_nonexisting_chunk() -> Result<(), Box<dyn std::error::Error>> {
        let store = super::SqliteChunkStore::new_memory()?;
        let res = store.get_chunk(0, 10, 100)?;
        if res.is_some() {
            Err("there should be no chunk")?;
        }
        Ok(())
    }

    #[test]
    fn should_retrive_chunk() -> Result<(), Box<dyn std::error::Error>> {
        let mut store = super::SqliteChunkStore::new_memory()?;
        let chunk = Chunk {
            compressed_data: vec![],
        };
        store.set_chunk(0, 10, 100, &chunk)?;
        let (_, stored) = store.get_chunk(0, 10, 100)?.ok_or("no chunk found")?;
        if chunk.compressed_data != stored.compressed_data {
            Err("chunks don't match")?;
        }
        Ok(())
    }

    #[test]
    fn should_retrive_smallest_chunk() -> Result<(), Box<dyn std::error::Error>> {
        let mut store = super::SqliteChunkStore::new_memory()?;

        store.set_chunk(
            0,
            1,
            9,
            &Chunk {
                compressed_data: vec![2],
            },
        )?;
        store.set_chunk(
            0,
            0,
            50,
            &Chunk {
                compressed_data: vec![5, 6],
            },
        )?;
        store.set_chunk(
            0,
            50,
            200,
            &Chunk {
                compressed_data: vec![5, 6],
            },
        )?;
        store.set_chunk(
            0,
            10,
            100,
            &Chunk {
                compressed_data: vec![],
            },
        )?;
        store.set_chunk(
            0,
            0,
            1000,
            &Chunk {
                compressed_data: vec![1],
            },
        )?;
        let (_, stored) = store.get_chunk(0, 10, 100)?.ok_or("no chunk found")?;
        if stored.compressed_data != vec![] {
            Err("chunks don't match")?;
        }
        Ok(())
    }
}
