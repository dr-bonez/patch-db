use std::sync::Arc;

use futures::future::{BoxFuture, FutureExt};
use json_ptr::{JsonPointer, SegList};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::broadcast::error::TryRecvError;
use tokio::sync::broadcast::Receiver;
use tokio::sync::{RwLock, RwLockReadGuard};

use crate::locker::{LockType, Locker, LockerGuard};
use crate::patch::{DiffPatch, Revision};
use crate::store::{PatchDb, Store};
use crate::Error;

pub trait Checkpoint: Sized {
    fn rebase(&mut self) -> Result<(), Error>;
    fn exists<'a, S: AsRef<str> + Send + Sync + 'a, V: SegList + Send + Sync + 'a>(
        &'a mut self,
        ptr: &'a JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'a, Store>>,
    ) -> BoxFuture<'a, Result<bool, Error>>;
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
    pub(crate) db: PatchDb,
    pub(crate) locks: Vec<(JsonPointer, LockerGuard)>,
    pub(crate) updates: DiffPatch,
    pub(crate) sub: Receiver<Arc<Revision>>,
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
    async fn exists<S: AsRef<str>, V: SegList>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'_, Store>>,
    ) -> Result<bool, Error> {
        let exists = {
            let store_lock = self.db.store.clone();
            let store = if let Some(store_read_lock) = store_read_lock {
                store_read_lock
            } else {
                store_lock.read().await
            };
            self.rebase()?;
            ptr.get(store.get_data()?).unwrap_or(&Value::Null) != &Value::Null
        };
        Ok(self.updates.for_path(ptr).exists().unwrap_or(exists))
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
        json_patch::patch(&mut data, &*self.updates.for_path(ptr))?;
        Ok(data)
    }
    async fn put_value<S: AsRef<str>, V: SegList>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &Value,
    ) -> Result<(), Error> {
        let old = Transaction::get_value(self, ptr, None).await?;
        let mut patch = crate::patch::diff(&old, &value);
        patch.prepend(ptr);
        self.updates.append(patch);
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
            updates: DiffPatch::default(),
            sub,
        })
    }
}
impl<'a> Checkpoint for &'a mut Transaction {
    fn rebase(&mut self) -> Result<(), Error> {
        Transaction::rebase(self)
    }
    fn exists<'b, S: AsRef<str> + Send + Sync + 'b, V: SegList + Send + Sync + 'b>(
        &'b mut self,
        ptr: &'b JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'b, Store>>,
    ) -> BoxFuture<'b, Result<bool, Error>> {
        Transaction::exists(self, ptr, store_read_lock).boxed()
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
        self.updates.append(patch)
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
    async fn exists<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'_, Store>>,
    ) -> Result<bool, Error> {
        let exists = {
            let store_lock = self.parent.store();
            let store = if let Some(store_read_lock) = store_read_lock {
                store_read_lock
            } else {
                store_lock.read().await
            };
            self.rebase()?;
            self.parent.exists(ptr, Some(store)).await?
        };
        Ok(self.updates.for_path(ptr).exists().unwrap_or(exists))
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
        json_patch::patch(&mut data, &*self.updates.for_path(ptr))?;
        Ok(data)
    }
    async fn put_value<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &Value,
    ) -> Result<(), Error> {
        let old = SubTransaction::get_value(self, ptr, None).await?;
        let mut patch = crate::patch::diff(&old, &value);
        patch.prepend(ptr);
        self.updates.append(patch);
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
            updates: DiffPatch::default(),
            sub,
        })
    }
}
impl<'a, Tx: Checkpoint + Send + Sync> Checkpoint for &'a mut SubTransaction<Tx> {
    fn rebase(&mut self) -> Result<(), Error> {
        SubTransaction::rebase(self)
    }
    fn exists<'b, S: AsRef<str> + Send + Sync + 'b, V: SegList + Send + Sync + 'b>(
        &'b mut self,
        ptr: &'b JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'b, Store>>,
    ) -> BoxFuture<'b, Result<bool, Error>> {
        SubTransaction::exists(self, ptr, store_read_lock).boxed()
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
        self.updates.append(patch)
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
