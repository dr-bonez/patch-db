use std::fs::OpenOptions;
use std::io::Error as IOError;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};

use fd_lock_rs::FdLock;
use futures::future::{BoxFuture, FutureExt};
use json_patch::{AddOperation, Patch, PatchOperation, RemoveOperation, ReplaceOperation};
use json_ptr::{JsonPointer, SegList};
use lazy_static::lazy_static;
use qutex_2::{Guard, QrwLock, Qutex, ReadGuard, WriteGuard};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::{
    fs::File,
    sync::{
        broadcast::{error::TryRecvError, Receiver, Sender},
        Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard,
    },
};

// note: inserting into an array (before another element) without proper locking can result in unexpected behaviour

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
    #[error("Subscriber Error: {0}")]
    Subscriber(#[from] TryRecvError),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Revision {
    pub id: u64,
    pub patch: DiffPatch,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DiffPatch(Patch);
impl DiffPatch {
    // safe to assume dictionary style symantics for arrays since patches will always be rebased before being applied
    pub fn for_path<S: AsRef<str>, V: SegList>(&self, ptr: &JsonPointer<S, V>) -> DiffPatch {
        let DiffPatch(Patch(ops)) = self;
        DiffPatch(Patch(
            ops.iter()
                .filter_map(|op| match op {
                    PatchOperation::Add(op) => {
                        if let Some(tail) = op.path.strip_prefix(ptr) {
                            Some(PatchOperation::Add(AddOperation {
                                path: tail.to_owned(),
                                value: op.value.clone(),
                            }))
                        } else if let Some(tail) = ptr.strip_prefix(&op.path) {
                            Some(PatchOperation::Add(AddOperation {
                                path: Default::default(),
                                value: tail.get(&op.value).cloned().unwrap_or_default(),
                            }))
                        } else {
                            None
                        }
                    }
                    PatchOperation::Replace(op) => {
                        if let Some(tail) = op.path.strip_prefix(ptr) {
                            Some(PatchOperation::Replace(ReplaceOperation {
                                path: tail.to_owned(),
                                value: op.value.clone(),
                            }))
                        } else if let Some(tail) = ptr.strip_prefix(&op.path) {
                            Some(PatchOperation::Replace(ReplaceOperation {
                                path: Default::default(),
                                value: tail.get(&op.value).cloned().unwrap_or_default(),
                            }))
                        } else {
                            None
                        }
                    }
                    PatchOperation::Remove(op) => {
                        if ptr.starts_with(&op.path) {
                            Some(PatchOperation::Replace(ReplaceOperation {
                                path: Default::default(),
                                value: Default::default(),
                            }))
                        } else if let Some(tail) = op.path.strip_prefix(ptr) {
                            Some(PatchOperation::Remove(RemoveOperation {
                                path: tail.to_owned(),
                            }))
                        } else {
                            None
                        }
                    }
                    _ => unreachable!(),
                })
                .collect(),
        ))
    }
    pub fn rebase(&mut self, onto: &DiffPatch) {
        let DiffPatch(Patch(ops)) = self;
        let DiffPatch(Patch(onto_ops)) = onto;
        for onto_op in onto_ops {
            if let PatchOperation::Add(onto_op) = onto_op {
                let arr_path_idx = onto_op.path.len() - 1;
                if let Some(onto_idx) = onto_op
                    .path
                    .get_segment(arr_path_idx)
                    .and_then(|seg| seg.parse::<usize>().ok())
                {
                    let prefix = onto_op.path.slice(..arr_path_idx).unwrap_or_default();
                    for op in ops.iter_mut() {
                        let path = match op {
                            PatchOperation::Add(op) => &mut op.path,
                            PatchOperation::Replace(op) => &mut op.path,
                            PatchOperation::Remove(op) => &mut op.path,
                            _ => unreachable!(),
                        };
                        if path.starts_with(&prefix) {
                            if let Some(idx) = path
                                .get_segment(arr_path_idx)
                                .and_then(|seg| seg.parse::<usize>().ok())
                            {
                                if idx >= onto_idx {
                                    let mut new_path = prefix.clone().to_owned();
                                    new_path.push_end_idx(idx + 1);
                                    if let Some(tail) = path.slice(arr_path_idx + 1..) {
                                        new_path.append(&tail);
                                    }
                                    *path = new_path;
                                }
                            }
                        }
                    }
                }
            } else if let PatchOperation::Remove(onto_op) = onto_op {
                let arr_path_idx = onto_op.path.len() - 1;
                if let Some(onto_idx) = onto_op
                    .path
                    .get_segment(arr_path_idx)
                    .and_then(|seg| seg.parse::<usize>().ok())
                {
                    let prefix = onto_op.path.slice(..arr_path_idx).unwrap_or_default();
                    for op in ops.iter_mut() {
                        let path = match op {
                            PatchOperation::Add(op) => &mut op.path,
                            PatchOperation::Replace(op) => &mut op.path,
                            PatchOperation::Remove(op) => &mut op.path,
                            _ => unreachable!(),
                        };
                        if path.starts_with(&prefix) {
                            if let Some(idx) = path
                                .get_segment(arr_path_idx)
                                .and_then(|seg| seg.parse::<usize>().ok())
                            {
                                if idx >= onto_idx {
                                    let mut new_path = prefix.clone().to_owned();
                                    new_path.push_end_idx(idx - 1);
                                    if let Some(tail) = path.slice(arr_path_idx + 1..) {
                                        new_path.append(&tail);
                                    }
                                    *path = new_path;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

lazy_static! {
    static ref OPEN_STORES: Mutex<HashMap<PathBuf, Qutex<()>>> = Mutex::new(HashMap::new());
}

pub struct Store {
    file: FdLock<File>,
    _lock: Guard<()>,
    cache_corrupted: Option<Arc<IOError>>,
    data: Value,
    revision: u64,
}
impl Store {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        if !path.as_ref().exists() {
            tokio::fs::File::create(path.as_ref()).await?;
        }
        let path = tokio::fs::canonicalize(path).await?;
        let _lock = {
            let mut lock = OPEN_STORES.lock().await;
            if let Some(open) = lock.get(&path) {
                open.clone().lock().await.unwrap()
            } else {
                let tex = Qutex::new(());
                lock.insert(path.clone(), tex.clone());
                tex.lock().await.unwrap()
            }
        };
        Ok(tokio::task::spawn_blocking(move || {
            use std::io::Write;

            let bak = path.with_extension("bak");
            if bak.exists() {
                std::fs::rename(&bak, &path)?;
            }
            let mut f = FdLock::lock(
                OpenOptions::new()
                    .create(true)
                    .read(true)
                    .append(true)
                    .open(&path)?,
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
            let bak_tmp = bak.with_extension("bak.tmp");
            let mut backup_file = std::fs::File::create(&bak_tmp)?;
            serde_cbor::to_writer(&mut backup_file, &revision)?;
            serde_cbor::to_writer(&mut backup_file, &data)?;
            backup_file.flush()?;
            backup_file.sync_all()?;
            std::fs::rename(&bak_tmp, &bak)?;
            nix::unistd::ftruncate(std::os::unix::io::AsRawFd::as_raw_fd(&*f), 0)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            serde_cbor::to_writer(&mut *f, &revision)?;
            serde_cbor::to_writer(&mut *f, &data)?;
            f.flush()?;
            f.sync_all()?;
            std::fs::remove_file(&bak)?;

            Ok::<_, Error>(Store {
                file: f.map(File::from_std),
                _lock,
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
        let res = Arc::new(Revision { id, patch });

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
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let (subscriber, _) = tokio::sync::broadcast::channel(16); // TODO: make this unbounded

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
        let mut store = self.store.write().await;
        let rev = store.put(ptr, value).await?;
        self.subscriber.send(rev.clone()).unwrap_or_default();
        Ok(rev)
    }
    pub async fn apply(
        &self,
        patch: DiffPatch,
        store_write_lock: Option<RwLockWriteGuard<'_, Store>>,
    ) -> Result<Arc<Revision>, Error> {
        let mut store = if let Some(store_write_lock) = store_write_lock {
            store_write_lock
        } else {
            self.store.write().await
        };
        let rev = store.apply(patch).await?;
        self.subscriber.send(rev.clone()).unwrap_or_default(); // ignore errors
        Ok(rev)
    }
    pub fn subscribe(&self) -> Receiver<Arc<Revision>> {
        self.subscriber.subscribe()
    }
    pub fn begin(&self) -> Transaction {
        Transaction {
            db: self.clone(),
            locks: Vec::new(),
            updates: DiffPatch(Patch(Vec::new())),
            sub: self.subscribe(),
        }
    }
}
pub trait Checkpoint: Sized {
    fn rebase(&mut self) -> Result<(), Error>;
    fn get_value<'a, S: AsRef<str> + Send + Sync + 'a, V: SegList + Send + Sync + 'a>(
        &'a mut self,
        ptr: &'a JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'a, Store>>,
    ) -> BoxFuture<'a, Result<Value, Error>>;
    fn put_value<'a, S: AsRef<str> + Send + Sync + 'a, V: SegList + Send + Sync + 'a>(
        &'a mut self,
        ptr: &'a JsonPointer<S, V>,
        value: &'a Value,
    ) -> BoxFuture<'a, Result<(), Error>>;
    fn store(&self) -> Arc<RwLock<Store>>;
    fn subscribe(&self) -> Receiver<Arc<Revision>>;
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
    sub: Receiver<Arc<Revision>>,
}
impl Transaction {
    pub fn rebase(&mut self) -> Result<(), Error> {
        while let Some(rev) = match self.sub.try_recv() {
            Ok(a) => Some(a),
            Err(TryRecvError::Empty) => None,
            Err(e) => return Err(e.into()),
        } {
            self.updates.rebase(&rev.patch);
        }
        Ok(())
    }
    async fn get_value<S: AsRef<str>, V: SegList>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'_, Store>>,
    ) -> Result<Value, Error> {
        let mut data = {
            let store_lock = self.db.store.clone();
            let store = if let Some(store_read_lock) = store_read_lock {
                store_read_lock
            } else {
                store_lock.read().await
            };
            self.rebase()?;
            ptr.get(store.get_data()?).cloned().unwrap_or_default()
        };
        json_patch::patch(&mut data, &self.updates.for_path(ptr).0)?;
        Ok(data)
    }
    async fn put_value<S: AsRef<str>, V: SegList>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &Value,
    ) -> Result<(), Error> {
        let old = Transaction::get_value(self, ptr, None).await?;
        let mut patch = json_patch::diff(&old, &value);
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
            Transaction::get_value(self, ptr, None).await?,
        )?)
    }
    pub async fn put<T: Serialize, S: AsRef<str>, V: SegList>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &T,
    ) -> Result<(), Error> {
        Transaction::put_value(self, ptr, &serde_json::to_value(value)?).await
    }
    pub async fn commit(mut self) -> Result<Arc<Revision>, Error> {
        let store_lock = self.db.store.clone();
        let store = store_lock.write().await;
        self.rebase()?;
        self.db.apply(self.updates, Some(store)).await
    }
    pub async fn begin(&mut self) -> Result<SubTransaction<&mut Self>, Error> {
        let store_lock = self.db.store.clone();
        let store = store_lock.read().await;
        self.rebase()?;
        let sub = self.db.subscribe();
        drop(store);
        Ok(SubTransaction {
            parent: self,
            locks: Vec::new(),
            updates: DiffPatch(Patch(Vec::new())),
            sub,
        })
    }
}
impl<'a> Checkpoint for &'a mut Transaction {
    fn rebase(&mut self) -> Result<(), Error> {
        Transaction::rebase(self)
    }
    fn get_value<'b, S: AsRef<str> + Send + Sync + 'b, V: SegList + Send + Sync + 'b>(
        &'b mut self,
        ptr: &'b JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'b, Store>>,
    ) -> BoxFuture<'b, Result<Value, Error>> {
        Transaction::get_value(self, ptr, store_read_lock).boxed()
    }
    fn put_value<'b, S: AsRef<str> + Send + Sync + 'b, V: SegList + Send + Sync + 'b>(
        &'b mut self,
        ptr: &'b JsonPointer<S, V>,
        value: &'b Value,
    ) -> BoxFuture<'b, Result<(), Error>> {
        Transaction::put_value(self, ptr, value).boxed()
    }
    fn store(&self) -> Arc<RwLock<Store>> {
        self.db.store.clone()
    }
    fn subscribe(&self) -> Receiver<Arc<Revision>> {
        self.db.subscribe()
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
    sub: Receiver<Arc<Revision>>,
}
impl<Tx: Checkpoint + Send + Sync> SubTransaction<Tx> {
    pub fn rebase(&mut self) -> Result<(), Error> {
        self.parent.rebase()?;
        while let Some(rev) = match self.sub.try_recv() {
            Ok(a) => Some(a),
            Err(TryRecvError::Empty) => None,
            Err(e) => return Err(e.into()),
        } {
            self.updates.rebase(&rev.patch);
        }
        Ok(())
    }
    async fn get_value<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'_, Store>>,
    ) -> Result<Value, Error> {
        let mut data = {
            let store_lock = self.parent.store();
            let store = if let Some(store_read_lock) = store_read_lock {
                store_read_lock
            } else {
                store_lock.read().await
            };
            self.rebase()?;
            self.parent.get_value(ptr, Some(store)).await?
        };
        json_patch::patch(&mut data, &self.updates.for_path(ptr).0)?;
        Ok(data)
    }
    async fn put_value<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &Value,
    ) -> Result<(), Error> {
        let old = SubTransaction::get_value(self, ptr, None).await?;
        let mut patch = json_patch::diff(&old, &value);
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
            SubTransaction::get_value(self, ptr, None).await?,
        )?)
    }
    pub async fn put<T: Serialize, S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &T,
    ) -> Result<(), Error> {
        SubTransaction::put_value(self, ptr, &serde_json::to_value(value)?).await
    }
    pub async fn commit(mut self) -> Result<(), Error> {
        let store_lock = self.parent.store();
        let store = store_lock.read().await;
        self.rebase()?;
        self.parent.apply(self.updates);
        drop(store);
        Ok(())
    }
    pub async fn begin(&mut self) -> Result<SubTransaction<&mut Self>, Error> {
        let store_lock = self.parent.store();
        let store = store_lock.read().await;
        self.rebase()?;
        let sub = self.parent.subscribe();
        drop(store);
        Ok(SubTransaction {
            parent: self,
            locks: Vec::new(),
            updates: DiffPatch(Patch(Vec::new())),
            sub,
        })
    }
}
impl<'a, Tx: Checkpoint + Send + Sync> Checkpoint for &'a mut SubTransaction<Tx> {
    fn rebase(&mut self) -> Result<(), Error> {
        SubTransaction::rebase(self)
    }
    fn get_value<'b, S: AsRef<str> + Send + Sync + 'b, V: SegList + Send + Sync + 'b>(
        &'b mut self,
        ptr: &'b JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'b, Store>>,
    ) -> BoxFuture<'b, Result<Value, Error>> {
        SubTransaction::get_value(self, ptr, store_read_lock).boxed()
    }
    fn put_value<'b, S: AsRef<str> + Send + Sync + 'b, V: SegList + Send + Sync + 'b>(
        &'b mut self,
        ptr: &'b JsonPointer<S, V>,
        value: &'b Value,
    ) -> BoxFuture<'b, Result<(), Error>> {
        SubTransaction::put_value(self, ptr, value).boxed()
    }
    fn store(&self) -> Arc<RwLock<Store>> {
        self.parent.store()
    }
    fn subscribe(&self) -> Receiver<Arc<Revision>> {
        self.parent.subscribe()
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
        locks: &mut Vec<(JsonPointer, LockerGuard)>, // tx locks
        extra_locks: &mut [&mut [(JsonPointer, LockerGuard)]], // tx parent locks
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
                        let mut lock = guard.lock().await;
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

pub struct ModelData<T: Serialize + for<'de> Deserialize<'de>>(T);
impl<T: Serialize + for<'de> Deserialize<'de>> Deref for ModelData<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct ModelDataMut<T: Serialize + for<'de> Deserialize<'de>> {
    original: Value,
    current: T,
    ptr: JsonPointer,
}
impl<T: Serialize + for<'de> Deserialize<'de>> ModelDataMut<T> {
    pub async fn save<Tx: Checkpoint>(self, tx: &mut Tx) -> Result<(), Error> {
        let current = serde_json::to_value(&self.current)?;
        let mut diff = DiffPatch(json_patch::diff(&self.original, &current));
        let target = tx.get_value(&self.ptr, None).await?;
        diff.rebase(&DiffPatch(json_patch::diff(&self.original, &target)));
        diff.0.prepend(&self.ptr);
        tx.apply(diff);
        Ok(())
    }
}
impl<T: Serialize + for<'de> Deserialize<'de>> Deref for ModelDataMut<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.current
    }
}
impl<T: Serialize + for<'de> Deserialize<'de>> DerefMut for ModelDataMut<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.current
    }
}

#[derive(Debug)]
pub struct Model<T: Serialize + for<'de> Deserialize<'de>> {
    ptr: JsonPointer,
    phantom: PhantomData<T>,
}
impl<T> Model<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    pub fn new(ptr: JsonPointer) -> Self {
        Self {
            ptr,
            phantom: PhantomData,
        }
    }

    pub async fn lock<Tx: Checkpoint>(&self, tx: &mut Tx, lock: LockType) {
        tx.lock(&self.ptr, lock).await
    }

    pub async fn get<Tx: Checkpoint>(&self, tx: &mut Tx) -> Result<ModelData<T>, Error> {
        Ok(ModelData(tx.get(&self.ptr, LockType::Read).await?))
    }

    pub async fn get_mut<Tx: Checkpoint>(&self, tx: &mut Tx) -> Result<ModelDataMut<T>, Error> {
        self.lock(tx, LockType::Write).await;
        let original = tx.get_value(&self.ptr, None).await?;
        let current = serde_json::from_value(original.clone())?;
        Ok(ModelDataMut {
            original,
            current,
            ptr: self.ptr.clone(),
        })
    }

    pub fn child<C: Serialize + for<'de> Deserialize<'de>>(&self, index: &str) -> Model<C> {
        let mut ptr = self.ptr.clone();
        ptr.push_end(index);
        Model {
            ptr,
            phantom: PhantomData,
        }
    }
}
impl<T> std::clone::Clone for Model<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fn clone(&self) -> Self {
        Model {
            ptr: self.ptr.clone(),
            phantom: PhantomData,
        }
    }
}

pub trait HasModel {
    type Model;
}
