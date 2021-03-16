use std::collections::HashMap;
use std::fs::OpenOptions;
use std::future::Future;
use std::io::Error as IOError;
use std::path::Path;
use std::sync::Arc;

use fd_lock_rs::FdLock;
use futures::future::{BoxFuture, FutureExt};
use json_patch::{Patch, PatchOperation};
use json_ptr::{JsonPointer, SegList};
use qutex_2::{QrwLock, ReadGuard, WriteGuard};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::{
    fs::File,
    sync::{
        broadcast::{Receiver, Sender},
        Mutex, RwLock,
    },
};

#[cfg(test)]
mod test;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO Error: {0}")]
    IO(#[from] IOError),
    #[error("JSON (De)Serialization Error: {0}")]
    JSON(#[from] serde_json::Error),
    #[error("CBOR (De)Serialization Error: {0}")]
    CBOR(#[from] serde_cbor::Error),
    #[error("Index Error: {0:?}")]
    Pointer(#[from] json_ptr::IndexError),
    #[error("Patch Error: {0}")]
    Patch(#[from] json_patch::PatchError),
    #[error("Join Error: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("FD Lock Error: {0}")]
    FDLock(#[from] fd_lock_rs::Error),
    #[error("Database Cache Corrupted: {0}")]
    CacheCorrupted(Arc<IOError>),
    #[error("Mutex Error (should be unreachable): {0}")]
    MutexError(#[from] tokio::sync::TryLockError),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Revision {
    pub id: u64,
    pub patch: Patch,
}

#[derive(Debug, Clone)]
pub struct DiffPatch(Patch);

pub struct Store {
    file: FdLock<File>,
    cache_corrupted: Option<Arc<IOError>>,
    data: Value,
    revision: u64,
}
impl Store {
    pub async fn open<P: AsRef<Path> + Send + 'static>(path: P) -> Result<Self, Error> {
        Ok(tokio::task::spawn_blocking(move || {
            use std::io::Write;

            let p = path.as_ref();
            let bak = p.with_extension("bak");
            if bak.exists() {
                std::fs::rename(&bak, p)?;
            }
            let mut f = FdLock::lock(
                OpenOptions::new()
                    .create(true)
                    .read(true)
                    .append(true)
                    .open(p)?,
                fd_lock_rs::LockType::Exclusive,
                true,
            )?;
            let mut stream =
                serde_cbor::StreamDeserializer::new(serde_cbor::de::IoRead::new(&mut *f));
            let mut revision: u64 = stream.next().transpose()?.unwrap_or(0);
            let mut stream = stream.change_output_type();
            let mut data = stream.next().transpose()?.unwrap_or_else(|| Value::Null);
            let mut stream = stream.change_output_type();
            while let Some(Ok(patch)) = stream.next() {
                json_patch::patch(&mut data, &patch)?;
                revision += 1;
            }
            serde_cbor::to_writer(std::fs::File::create(&bak)?, &data)?;
            nix::unistd::ftruncate(std::os::unix::io::AsRawFd::as_raw_fd(&*f), 0)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            serde_cbor::to_writer(&mut *f, &revision)?;
            serde_cbor::to_writer(&mut *f, &data)?;
            f.flush()?;
            f.sync_all()?;
            std::fs::remove_file(&bak)?;

            Ok::<_, Error>(Store {
                file: f.map(File::from_std),
                cache_corrupted: None,
                data,
                revision,
            })
        })
        .await??)
    }
    fn check_cache_corrupted(&self) -> Result<(), Error> {
        if let Some(ref err) = self.cache_corrupted {
            Err(Error::CacheCorrupted(err.clone()))
        } else {
            Ok(())
        }
    }
    fn get_data(&self) -> Result<&Value, Error> {
        self.check_cache_corrupted()?;
        Ok(&self.data)
    }
    fn get_data_mut(&mut self) -> Result<&mut Value, Error> {
        self.check_cache_corrupted()?;
        Ok(&mut self.data)
    }
    pub async fn close(mut self) -> Result<(), Error> {
        use tokio::io::AsyncWriteExt;

        self.file.flush().await?;
        self.file.shutdown().await?;
        self.file.unlock(true).map_err(|e| e.1)?;
        Ok(())
    }
    pub fn get<T: for<'de> Deserialize<'de>, S: AsRef<str>, V: SegList>(
        &self,
        ptr: &JsonPointer<S, V>,
    ) -> Result<T, Error> {
        Ok(serde_json::from_value(
            ptr.get(self.get_data()?).unwrap_or(&Value::Null).clone(),
        )?)
    }
    pub fn dump(&self) -> Value {
        self.get_data().unwrap().clone()
    }
    pub async fn put<T: Serialize, S: AsRef<str>, V: SegList>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &T,
    ) -> Result<Arc<Revision>, Error> {
        let mut patch = DiffPatch(json_patch::diff(
            ptr.get(self.get_data()?).unwrap_or(&Value::Null),
            &serde_json::to_value(value)?,
        ));
        patch.0.prepend(ptr);
        self.apply(patch).await
    }
    pub async fn apply(&mut self, patch: DiffPatch) -> Result<Arc<Revision>, Error> {
        use tokio::io::AsyncWriteExt;

        self.check_cache_corrupted()?;
        let patch_bin = serde_cbor::to_vec(&patch.0)?;
        json_patch::patch(self.get_data_mut()?, &patch.0)?;

        async fn sync_to_disk(file: &mut File, patch_bin: &[u8]) -> Result<(), IOError> {
            file.write_all(patch_bin).await?;
            file.flush().await?;
            file.sync_data().await?;
            Ok(())
        }
        if let Err(e) = sync_to_disk(&mut *self.file, &patch_bin).await {
            let e = Arc::new(e);
            self.cache_corrupted = Some(e.clone());
            return Err(Error::CacheCorrupted(e));
            // TODO: try to recover.
        }

        let id = self.revision;
        self.revision += 1;
        let res = Arc::new(Revision { id, patch: patch.0 });

        Ok(res)
    }
}

#[derive(Clone)]
pub struct PatchDb {
    store: Arc<RwLock<Store>>,
    subscriber: Arc<Sender<Arc<Revision>>>,
    locker: Locker,
}
impl PatchDb {
    pub async fn open<P: AsRef<Path> + Send + 'static>(path: P) -> Result<Self, Error> {
        let (subscriber, _) = tokio::sync::broadcast::channel(16);

        Ok(PatchDb {
            store: Arc::new(RwLock::new(Store::open(path).await?)),
            locker: Locker::new(),
            subscriber: Arc::new(subscriber),
        })
    }
    pub async fn get<T: for<'de> Deserialize<'de>, S: AsRef<str>, V: SegList>(
        &self,
        ptr: &JsonPointer<S, V>,
    ) -> Result<T, Error> {
        self.store.read().await.get(ptr)
    }
    pub async fn put<T: Serialize, S: AsRef<str>, V: SegList>(
        &self,
        ptr: &JsonPointer<S, V>,
        value: &T,
    ) -> Result<Arc<Revision>, Error> {
        self.store.write().await.put(ptr, value).await
    }
    pub async fn apply(&self, patch: DiffPatch) -> Result<Arc<Revision>, Error> {
        self.store.write().await.apply(patch).await
    }
    pub fn subscribe(&self) -> Receiver<Arc<Revision>> {
        self.subscriber.subscribe()
    }
    pub fn begin(&self) -> Transaction {
        Transaction {
            db: self.clone(),
            locks: Vec::new(),
            updates: DiffPatch(Patch(Vec::new())),
        }
    }
}
pub trait Checkpoint {
    type SubTx: Checkpoint;
    fn get_value<'a, S: AsRef<str> + Send + Sync + 'a, V: SegList + Send + Sync + 'a>(
        &'a mut self,
        ptr: &'a JsonPointer<S, V>,
    ) -> BoxFuture<'a, Result<Value, Error>>;
    fn put_value<'a, S: AsRef<str> + Send + Sync + 'a, V: SegList + Send + Sync + 'a>(
        &'a mut self,
        ptr: &'a JsonPointer<S, V>,
        value: &'a Value,
    ) -> BoxFuture<'a, Result<(), Error>>;
    fn locker_and_locks(&mut self) -> (&Locker, Vec<&mut [(JsonPointer, LockerGuard)]>);
    fn apply(&mut self, patch: DiffPatch);
    fn lock<'a, S: AsRef<str> + Clone + Send + Sync, V: SegList + Clone + Send + Sync>(
        &'a mut self,
        ptr: &'a JsonPointer<S, V>,
        lock: LockType,
    ) -> BoxFuture<'a, ()>;
    fn get<
        'a,
        T: for<'de> Deserialize<'de> + 'a,
        S: AsRef<str> + Clone + Send + Sync + 'a,
        V: SegList + Clone + Send + Sync + 'a,
    >(
        &'a mut self,
        ptr: &'a JsonPointer<S, V>,
        lock: LockType,
    ) -> BoxFuture<'a, Result<T, Error>>;
    fn put<
        'a,
        T: Serialize + Send + Sync + 'a,
        S: AsRef<str> + Send + Sync + 'a,
        V: SegList + Send + Sync + 'a,
    >(
        &'a mut self,
        ptr: &'a JsonPointer<S, V>,
        value: &'a T,
    ) -> BoxFuture<'a, Result<(), Error>>;
}

pub struct Transaction {
    db: PatchDb,
    locks: Vec<(JsonPointer, LockerGuard)>,
    updates: DiffPatch,
}
impl Transaction {
    async fn get_value<S: AsRef<str>, V: SegList>(
        &mut self,
        ptr: &JsonPointer<S, V>,
    ) -> Result<Value, Error> {
        let mut data: Value = ptr
            .get(self.db.store.read().await.get_data()?)
            .unwrap_or(&Value::Null)
            .clone();
        for op in (self.updates.0).0.iter() {
            match op {
                PatchOperation::Add(ref op) => {
                    if let Some(path) = op.path.strip_prefix(ptr) {
                        path.insert(&mut data, op.value.clone(), false)?;
                    }
                }
                PatchOperation::Remove(ref op) => {
                    if let Some(path) = op.path.strip_prefix(ptr) {
                        path.remove(&mut data, false);
                    }
                }
                PatchOperation::Replace(ref op) => {
                    if let Some(path) = op.path.strip_prefix(ptr) {
                        path.set(&mut data, op.value.clone(), false)?;
                    }
                }
                _ => unreachable!("Diff patches cannot contain other operations."),
            }
        }
        Ok(data)
    }
    async fn put_value<S: AsRef<str>, V: SegList>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &Value,
    ) -> Result<(), Error> {
        let old = Transaction::get_value(self, ptr).await?;
        let mut patch = json_patch::diff(&old, value);
        patch.prepend(ptr);
        (self.updates.0).0.extend(patch.0);
        Ok(())
    }
    pub async fn lock<S: AsRef<str> + Clone, V: SegList + Clone>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        lock: LockType,
    ) {
        match lock {
            LockType::None => (),
            LockType::Read => {
                self.db
                    .locker
                    .add_read_lock(ptr, &mut self.locks, &mut [])
                    .await
            }
            LockType::Write => {
                self.db
                    .locker
                    .add_write_lock(ptr, &mut self.locks, &mut [])
                    .await
            }
        }
    }
    pub async fn get<T: for<'de> Deserialize<'de>, S: AsRef<str> + Clone, V: SegList + Clone>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        lock: LockType,
    ) -> Result<T, Error> {
        self.lock(ptr, lock).await;
        Ok(serde_json::from_value(
            Transaction::get_value(self, ptr).await?,
        )?)
    }
    pub async fn put<T: Serialize, S: AsRef<str>, V: SegList>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &T,
    ) -> Result<(), Error> {
        Transaction::put_value(self, ptr, &serde_json::to_value(value)?).await
    }
    pub async fn commit(self) -> Result<Arc<Revision>, Error> {
        self.db.apply(self.updates).await
    }
}
impl<'a> Checkpoint for &'a mut Transaction {
    type SubTx = &'a mut SubTransaction<Self>;
    fn get_value<'b, S: AsRef<str> + Send + Sync + 'b, V: SegList + Send + Sync + 'b>(
        &'b mut self,
        ptr: &'b JsonPointer<S, V>,
    ) -> BoxFuture<'b, Result<Value, Error>> {
        Transaction::get_value(self, ptr).boxed()
    }
    fn put_value<'b, S: AsRef<str> + Send + Sync + 'b, V: SegList + Send + Sync + 'b>(
        &'b mut self,
        ptr: &'b JsonPointer<S, V>,
        value: &'b Value,
    ) -> BoxFuture<'b, Result<(), Error>> {
        Transaction::put_value(self, ptr, value).boxed()
    }
    fn locker_and_locks(&mut self) -> (&Locker, Vec<&mut [(JsonPointer, LockerGuard)]>) {
        (&self.db.locker, vec![&mut self.locks])
    }
    fn apply(&mut self, patch: DiffPatch) {
        (self.updates.0).0.extend((patch.0).0)
    }
    fn lock<'b, S: AsRef<str> + Clone + Send + Sync, V: SegList + Clone + Send + Sync>(
        &'b mut self,
        ptr: &'b JsonPointer<S, V>,
        lock: LockType,
    ) -> BoxFuture<'b, ()> {
        Transaction::lock(self, ptr, lock).boxed()
    }
    fn get<
        'b,
        T: for<'de> Deserialize<'de> + 'b,
        S: AsRef<str> + Clone + Send + Sync + 'b,
        V: SegList + Clone + Send + Sync + 'b,
    >(
        &'b mut self,
        ptr: &'b JsonPointer<S, V>,
        lock: LockType,
    ) -> BoxFuture<'b, Result<T, Error>> {
        Transaction::get(self, ptr, lock).boxed()
    }
    fn put<
        'b,
        T: Serialize + Send + Sync + 'b,
        S: AsRef<str> + Send + Sync + 'b,
        V: SegList + Send + Sync + 'b,
    >(
        &'b mut self,
        ptr: &'b JsonPointer<S, V>,
        value: &'b T,
    ) -> BoxFuture<'b, Result<(), Error>> {
        Transaction::put(self, ptr, value).boxed()
    }
}

pub struct SubTransaction<Tx: Checkpoint> {
    parent: Tx,
    locks: Vec<(JsonPointer, LockerGuard)>,
    updates: DiffPatch,
}
impl<Tx: Checkpoint> SubTransaction<Tx> {
    async fn get_value<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
    ) -> Result<Value, Error> {
        let mut data: Value = self.parent.get_value(ptr).await?;
        for op in (self.updates.0).0.iter() {
            match op {
                PatchOperation::Add(ref op) => {
                    if let Some(path) = op.path.strip_prefix(ptr) {
                        path.insert(&mut data, op.value.clone(), false)?;
                    }
                }
                PatchOperation::Remove(ref op) => {
                    if let Some(path) = op.path.strip_prefix(ptr) {
                        path.remove(&mut data, false);
                    }
                }
                PatchOperation::Replace(ref op) => {
                    if let Some(path) = op.path.strip_prefix(ptr) {
                        path.set(&mut data, op.value.clone(), false)?;
                    }
                }
                _ => unreachable!("Diff patches cannot contain other operations."),
            }
        }
        Ok(data)
    }
    async fn put_value<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &Value,
    ) -> Result<(), Error> {
        let old = SubTransaction::get_value(self, ptr).await?;
        let mut patch = json_patch::diff(&old, value);
        patch.prepend(ptr);
        (self.updates.0).0.extend(patch.0);
        Ok(())
    }
    pub async fn lock<S: AsRef<str> + Clone, V: SegList + Clone>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        lock: LockType,
    ) {
        match lock {
            LockType::None => (),
            LockType::Read => {
                let (locker, mut locks) = self.parent.locker_and_locks();
                locker.add_read_lock(ptr, &mut self.locks, &mut locks).await
            }
            LockType::Write => {
                let (locker, mut locks) = self.parent.locker_and_locks();
                locker
                    .add_write_lock(ptr, &mut self.locks, &mut locks)
                    .await
            }
        }
    }
    pub async fn get<
        T: for<'de> Deserialize<'de>,
        S: AsRef<str> + Clone + Send + Sync,
        V: SegList + Clone + Send + Sync,
    >(
        &mut self,
        ptr: &JsonPointer<S, V>,
        lock: LockType,
    ) -> Result<T, Error> {
        self.lock(ptr, lock).await;
        Ok(serde_json::from_value(
            SubTransaction::get_value(self, ptr).await?,
        )?)
    }
    pub async fn put<T: Serialize, S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &T,
    ) -> Result<(), Error> {
        let old = SubTransaction::get_value(self, ptr).await?;
        let new = serde_json::to_value(value)?;
        let mut patch = json_patch::diff(&old, &new);
        patch.prepend(ptr);
        (self.updates.0).0.extend(patch.0);
        Ok(())
    }
    pub fn commit(mut self) {
        self.parent.apply(self.updates)
    }
}
impl<'a, Tx: Checkpoint + Send + Sync> Checkpoint for &'a mut SubTransaction<Tx> {
    type SubTx = &'a mut SubTransaction<Self>;
    fn get_value<'b, S: AsRef<str> + Send + Sync + 'b, V: SegList + Send + Sync + 'b>(
        &'b mut self,
        ptr: &'b JsonPointer<S, V>,
    ) -> BoxFuture<'b, Result<Value, Error>> {
        SubTransaction::get_value(self, ptr).boxed()
    }
    fn put_value<'b, S: AsRef<str> + Send + Sync + 'b, V: SegList + Send + Sync + 'b>(
        &'b mut self,
        ptr: &'b JsonPointer<S, V>,
        value: &'b Value,
    ) -> BoxFuture<'b, Result<(), Error>> {
        SubTransaction::put_value(self, ptr, value).boxed()
    }
    fn locker_and_locks(&mut self) -> (&Locker, Vec<&mut [(JsonPointer, LockerGuard)]>) {
        let (locker, mut locks) = self.parent.locker_and_locks();
        locks.push(&mut self.locks);
        (locker, locks)
    }
    fn apply(&mut self, patch: DiffPatch) {
        (self.updates.0).0.extend((patch.0).0)
    }
    fn lock<'b, S: AsRef<str> + Clone + Send + Sync, V: SegList + Clone + Send + Sync>(
        &'b mut self,
        ptr: &'b JsonPointer<S, V>,
        lock: LockType,
    ) -> BoxFuture<'b, ()> {
        SubTransaction::lock(self, ptr, lock).boxed()
    }
    fn get<
        'b,
        T: for<'de> Deserialize<'de> + 'b,
        S: AsRef<str> + Clone + Send + Sync + 'b,
        V: SegList + Clone + Send + Sync + 'b,
    >(
        &'b mut self,
        ptr: &'b JsonPointer<S, V>,
        lock: LockType,
    ) -> BoxFuture<'b, Result<T, Error>> {
        SubTransaction::get(self, ptr, lock).boxed()
    }
    fn put<
        'b,
        T: Serialize + Send + Sync + 'b,
        S: AsRef<str> + Send + Sync + 'b,
        V: SegList + Send + Sync + 'b,
    >(
        &'b mut self,
        ptr: &'b JsonPointer<S, V>,
        value: &'b T,
    ) -> BoxFuture<'b, Result<(), Error>> {
        SubTransaction::put(self, ptr, value).boxed()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum LockType {
    None,
    Read,
    Write,
}

pub enum LockerGuard {
    Empty,
    Read(LockerReadGuard),
    Write(LockerWriteGuard),
}
impl LockerGuard {
    pub fn take(&mut self) -> Self {
        std::mem::replace(self, LockerGuard::Empty)
    }
}

#[derive(Debug, Clone)]
pub struct LockerReadGuard(Arc<Mutex<Option<ReadGuard<HashMap<String, Locker>>>>>);
impl LockerReadGuard {
    async fn upgrade(&self) -> Option<LockerWriteGuard> {
        let guard = self.0.try_lock().unwrap().take();
        if let Some(g) = guard {
            Some(LockerWriteGuard(
                Some(ReadGuard::upgrade(g).await.unwrap()),
                Some(self.clone()),
            ))
        } else {
            None
        }
    }
}
impl From<ReadGuard<HashMap<String, Locker>>> for LockerReadGuard {
    fn from(guard: ReadGuard<HashMap<String, Locker>>) -> Self {
        LockerReadGuard(Arc::new(Mutex::new(Some(guard))))
    }
}

pub struct LockerWriteGuard(
    Option<WriteGuard<HashMap<String, Locker>>>,
    Option<LockerReadGuard>,
);
impl From<WriteGuard<HashMap<String, Locker>>> for LockerWriteGuard {
    fn from(guard: WriteGuard<HashMap<String, Locker>>) -> Self {
        LockerWriteGuard(Some(guard), None)
    }
}
impl Drop for LockerWriteGuard {
    fn drop(&mut self) {
        if let (Some(write), Some(read)) = (self.0.take(), self.1.take()) {
            *read.0.try_lock().unwrap() = Some(WriteGuard::downgrade(write));
        }
    }
}

#[derive(Clone, Debug)]
pub struct Locker(QrwLock<HashMap<String, Locker>>);
impl Locker {
    pub fn new() -> Self {
        Locker(QrwLock::new(HashMap::new()))
    }
    pub async fn lock_read<S: AsRef<str>, V: SegList>(
        &self,
        ptr: &JsonPointer<S, V>,
    ) -> ReadGuard<HashMap<String, Locker>> {
        let mut lock = Some(self.0.clone().read().await.unwrap());
        for seg in ptr.iter() {
            let new_lock = if let Some(locker) = lock.as_ref().unwrap().get(seg) {
                locker.0.clone().read().await.unwrap()
            } else {
                let mut writer = ReadGuard::upgrade(lock.take().unwrap()).await.unwrap();
                writer.insert(seg.to_owned(), Locker::new());
                let reader = WriteGuard::downgrade(writer);
                reader.get(seg).unwrap().0.clone().read().await.unwrap()
            };
            lock = Some(new_lock);
        }
        lock.unwrap()
    }
    async fn add_read_lock<S: AsRef<str> + Clone, V: SegList + Clone>(
        &self,
        ptr: &JsonPointer<S, V>,
        locks: &mut Vec<(JsonPointer, LockerGuard)>,
        extra_locks: &mut [&mut [(JsonPointer, LockerGuard)]],
    ) {
        for lock in extra_locks
            .iter()
            .flat_map(|a| a.iter())
            .chain(locks.iter())
        {
            if ptr.starts_with(&lock.0) {
                return;
            }
        }
        locks.push((
            JsonPointer::to_owned(ptr.clone()),
            LockerGuard::Read(self.lock_read(ptr).await.into()),
        ));
    }
    pub async fn lock_write<S: AsRef<str>, V: SegList>(
        &self,
        ptr: &JsonPointer<S, V>,
    ) -> WriteGuard<HashMap<String, Locker>> {
        let mut lock = self.0.clone().write().await.unwrap();
        for seg in ptr.iter() {
            let new_lock = if let Some(locker) = lock.get(seg) {
                locker.0.clone().write().await.unwrap()
            } else {
                lock.insert(seg.to_owned(), Locker::new());
                lock.get(seg).unwrap().0.clone().write().await.unwrap()
            };
            lock = new_lock;
        }
        lock
    }
    async fn add_write_lock<S: AsRef<str> + Clone, V: SegList + Clone>(
        &self,
        ptr: &JsonPointer<S, V>,
        locks: &mut Vec<(JsonPointer, LockerGuard)>,
        extra_locks: &mut [&mut [(JsonPointer, LockerGuard)]],
    ) {
        let mut final_lock = None;
        for lock in extra_locks
            .iter_mut()
            .flat_map(|a| a.iter_mut())
            .chain(locks.iter_mut())
        {
            enum Choice {
                Return,
                Continue,
                Break,
            }
            let choice: Choice;
            if let Some(remainder) = ptr.strip_prefix(&lock.0) {
                let guard = lock.1.take();
                lock.1 = match guard {
                    LockerGuard::Read(LockerReadGuard(guard)) if !remainder.is_empty() => {
                        // read guard already exists at higher level
                        let mut lock = guard.try_lock().unwrap();
                        if let Some(l) = lock.take() {
                            let mut orig_lock = None;
                            let mut lock = ReadGuard::upgrade(l).await.unwrap();
                            for seg in remainder.iter() {
                                let new_lock = if let Some(locker) = lock.get(seg) {
                                    locker.0.clone().write().await.unwrap()
                                } else {
                                    lock.insert(seg.to_owned(), Locker::new());
                                    lock.get(seg).unwrap().0.clone().write().await.unwrap()
                                };
                                if orig_lock.is_none() {
                                    orig_lock = Some(lock);
                                }
                                lock = new_lock;
                            }
                            final_lock = Some(LockerGuard::Write(lock.into()));
                            choice = Choice::Break;
                            LockerGuard::Read(WriteGuard::downgrade(orig_lock.unwrap()).into())
                        } else {
                            drop(lock);
                            choice = Choice::Return;
                            LockerGuard::Read(LockerReadGuard(guard))
                        }
                    }
                    LockerGuard::Read(l) => {
                        // read exists, convert to write
                        if let Some(upgraded) = l.upgrade().await {
                            final_lock = Some(LockerGuard::Write(upgraded));
                            choice = Choice::Break;
                        } else {
                            choice = Choice::Continue;
                        }
                        LockerGuard::Read(l)
                    }
                    LockerGuard::Write(l) => {
                        choice = Choice::Return;
                        LockerGuard::Write(l)
                    } // leave it alone, already sufficiently locked
                    LockerGuard::Empty => {
                        unreachable!("LockerGuard found empty");
                    }
                };
                match choice {
                    Choice::Return => return,
                    Choice::Break => break,
                    Choice::Continue => continue,
                }
            }
        }
        locks.push((
            JsonPointer::to_owned(ptr.clone()),
            if let Some(lock) = final_lock {
                lock
            } else {
                LockerGuard::Write(self.lock_write(ptr).await.into())
            },
        ));
    }
}
impl Default for Locker {
    fn default() -> Self {
        Locker::new()
    }
}

pub trait ModelData: Send + Sync {
    type Inner;
    fn apply<
        'a,
        F: FnOnce(&mut Self::Inner) -> ResFut + Send + Sync + 'a,
        ResFut: Future<Output = Res> + Send + Sync + 'a,
        Res: Send + Sync + 'a,
    >(
        &'a mut self,
        f: F,
    ) -> BoxFuture<'a, Res>;
}

pub enum Never {}
impl ModelData for Never {
    type Inner = Never;
    fn apply<
        'a,
        F: FnOnce(&mut Self::Inner) -> ResFut + 'a,
        ResFut: Future<Output = Res> + 'a,
        Res: 'a,
    >(
        &'a mut self,
        _: F,
    ) -> BoxFuture<'a, Res> {
        match *self {}
    }
}

pub enum GenericModelData<T: Serialize + for<'de> Deserialize<'de>, Parent: ModelData> {
    Uninitialized(Vec<ChildHooks<T>>),
    Ref(
        Arc<dyn Fn(&mut Parent::Inner) -> &mut T + Send + Sync>,
        Arc<Mutex<Parent>>,
    ),
    Owned(Box<T>),
}
impl<T: Serialize + for<'de> Deserialize<'de>, Parent: ModelData> GenericModelData<T, Parent> {
    pub fn is_initialized(&self) -> bool {
        match self {
            GenericModelData::Uninitialized(_) => false,
            _ => true,
        }
    }
    async fn apply<
        F: FnOnce(&mut T) -> ResFut + Send + Sync,
        ResFut: Future<Output = Res> + Send + Sync,
        Res: Send + Sync,
    >(
        &mut self,
        f: F,
    ) -> Res {
        match self {
            GenericModelData::Owned(data) => f(data).await,
            GenericModelData::Ref(g, parent) => parent.lock().await.apply(|t| f(g(t))).await,
            _ => panic!("uninitialized"),
        }
    }
}
impl<T: Serialize + for<'de> Deserialize<'de> + Send + Sync, Parent: ModelData> ModelData
    for GenericModelData<T, Parent>
{
    type Inner = T;
    fn apply<
        'a,
        F: FnOnce(&mut Self::Inner) -> ResFut + Send + Sync + 'a,
        ResFut: Future<Output = Res> + Send + Sync + 'a,
        Res: Send + Sync + 'a,
    >(
        &'a mut self,
        f: F,
    ) -> BoxFuture<'a, Res> {
        self.apply(f).boxed()
    }
}

pub struct ChildHooks<T> {
    init_parent: Box<dyn for<'a> FnOnce(&'a mut T) -> BoxFuture<'a, ()> + Send + Sync>,
    save_data: Box<
        dyn Fn() -> BoxFuture<'static, Result<Vec<(JsonPointer, Value)>, serde_json::Error>>
            + Send
            + Sync,
    >,
}
impl<T> ChildHooks<T> {
    async fn init_parent(self, parent: &mut T) {
        (self.init_parent)(parent).await
    }
    async fn save_data(&self) -> Result<Vec<(JsonPointer, Value)>, serde_json::Error> {
        (self.save_data)().await
    }
}

pub struct GenericModel<T: Serialize + for<'de> Deserialize<'de>, Parent: ModelData> {
    data: Arc<Mutex<GenericModelData<T, Parent>>>,
    ptr: JsonPointer,
}
impl<
        'a,
        T: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        Parent: ModelData + 'static,
    > GenericModel<T, Parent>
{
    pub fn new(ptr: JsonPointer) -> Self {
        Self {
            data: Arc::new(Mutex::new(GenericModelData::Uninitialized(Vec::new()))),
            ptr,
        }
    }

    /// locks
    async fn fetch<Tx: Checkpoint>(&mut self, tx: &mut Tx) -> Result<(), Error> {
        let mut data = self.data.lock().await;
        if let GenericModelData::Uninitialized(children) = &mut *data {
            let mut a: Box<T> = tx.get(&self.ptr, LockType::None).await?;
            for child in std::mem::replace(children, Vec::new()) {
                child.init_parent(&mut a).await;
            }
            *data = GenericModelData::Owned(a);
        }

        Ok(())
    }

    pub async fn lock<Tx: Checkpoint>(&self, tx: &mut Tx, lock: LockType) {
        tx.lock(&self.ptr, lock).await
    }

    /// locks
    pub async fn peek<
        Tx: Checkpoint,
        F: FnOnce(&T) -> ResFut + Send + Sync,
        ResFut: Future<Output = Res> + Send + Sync,
        Res: Send + Sync,
    >(
        &mut self,
        tx: &mut Tx,
        f: F,
    ) -> Result<Res, Error> {
        self.fetch(tx).await?;
        Ok(self.data.lock().await.apply(|t| f(t)).await)
    }

    /// locks
    pub async fn apply<
        Tx: Checkpoint,
        F: FnOnce(&mut T) -> ResFut + Send + Sync,
        ResFut: Future<Output = Res> + Send + Sync,
        Res: Send + Sync,
    >(
        &mut self,
        tx: &mut Tx,
        f: F,
    ) -> Result<Res, Error> {
        self.fetch(tx).await?;
        Ok(self.data.lock().await.apply(f).await)
    }

    /// locks
    pub async fn child<C, F, S, V>(
        &mut self,
        path: &JsonPointer<S, V>,
        f: F,
    ) -> GenericModel<C, GenericModelData<T, Parent>>
    where
        C: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        F: Fn(&mut T) -> &mut C + Send + Sync + 'static,
        S: AsRef<str>,
        V: SegList,
        for<'v> &'v V: IntoIterator<Item = &'v json_ptr::PtrSegment>,
    {
        let arc_f = Arc::new(f);
        let ptr = self.ptr.clone() + path;
        let mut data_ref = self.data.lock().await;
        let data = if let GenericModelData::Uninitialized(children) = &mut *data_ref {
            let data = Arc::new(Mutex::new(GenericModelData::Uninitialized(Vec::new())));
            let init_parent_data = data.clone();
            let parent_data = self.data.clone();
            let save_data_data = data.clone();
            let save_data_ptr = ptr.clone();
            children.push(ChildHooks {
                init_parent: Box::new(move |parent| {
                    async move {
                        let mut data_ref = init_parent_data.lock().await;
                        let self_data = std::mem::replace(
                            &mut *data_ref,
                            GenericModelData::Uninitialized(Vec::new()),
                        );
                        match self_data {
                            GenericModelData::Owned(t) => *arc_f(parent) = *t,
                            GenericModelData::Uninitialized(children) => {
                                for child in children {
                                    child.init_parent(arc_f(parent)).await; // TODO: can probably parallelize
                                }
                            }
                            _ => (),
                        }
                        *data_ref = GenericModelData::Ref(arc_f, parent_data);
                    }
                    .boxed()
                }),
                save_data: Box::new(move || {
                    let ptr = save_data_ptr.clone();
                    let data = save_data_data.clone();
                    async move {
                        let mut data_ref = data.lock().await;
                        if let GenericModelData::Uninitialized(children) = &mut *data_ref {
                            let mut res = Vec::new();
                            for child in children {
                                res.append(&mut child.save_data().await?)
                            }
                            Ok(res)
                        } else {
                            Ok(vec![(
                                ptr,
                                data_ref
                                    .apply(|t| futures::future::ready(serde_json::to_value(t)))
                                    .await?,
                            )])
                        }
                    }
                    .boxed()
                }),
            });
            data
        } else {
            Arc::new(Mutex::new(GenericModelData::Ref(
                arc_f.clone(),
                self.data.clone(),
            )))
        };
        GenericModel { data, ptr }
    }

    /// locks
    pub async fn save<Tx: Checkpoint>(&mut self, tx: &mut Tx) -> Result<(), Error> {
        let mut data_ref = self.data.lock().await;
        for (ptr, value) in if let GenericModelData::Uninitialized(children) = &mut *data_ref {
            let mut res = Vec::new();
            for child in children {
                res.append(&mut child.save_data().await?)
            }
            res
        } else {
            vec![(
                self.ptr.clone(),
                data_ref
                    .apply(|t| futures::future::ready(serde_json::to_value(t)))
                    .await?,
            )]
        } {
            tx.put_value(&ptr, &value).await?;
        }
        Ok(())
    }
}
