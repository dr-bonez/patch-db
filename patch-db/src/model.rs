use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut, Index};

use json_ptr::JsonPointer;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::locker::LockType;
use crate::transaction::Checkpoint;
use crate::Error;

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
        let mut diff = crate::patch::diff(&self.original, &current);
        let target = tx.get_value(&self.ptr, None).await?;
        diff.rebase(&crate::patch::diff(&self.original, &target));
        diff.prepend(&self.ptr);
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
impl<T> From<JsonPointer> for Model<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fn from(ptr: JsonPointer) -> Self {
        Self {
            ptr,
            phantom: PhantomData,
        }
    }
}
impl<T> From<Model<T>> for JsonPointer
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fn from(model: Model<T>) -> Self {
        model.ptr
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
    type Model: From<JsonPointer>;
}

pub struct BoxModel<T: HasModel + Serialize + for<'de> Deserialize<'de>>(T::Model);
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> Deref for BoxModel<T> {
    type Target = T::Model;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> From<Model<Box<T>>> for BoxModel<T> {
    fn from(model: Model<Box<T>>) -> Self {
        BoxModel(T::Model::from(JsonPointer::from(model)))
    }
}
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> From<JsonPointer> for BoxModel<T> {
    fn from(ptr: JsonPointer) -> Self {
        BoxModel(T::Model::from(ptr))
    }
}
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> HasModel for Box<T> {
    type Model = BoxModel<T>;
}

pub struct VecModel<T: Serialize + for<'de> Deserialize<'de>>(Model<Vec<T>>);
impl<T: Serialize + for<'de> Deserialize<'de>> Deref for VecModel<T> {
    type Target = Model<Vec<T>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T: Serialize + for<'de> Deserialize<'de>> VecModel<T> {
    pub fn idx(&self, idx: usize) -> Model<Option<T>> {
        self.child(&format!("{}", idx))
    }
}
impl<T: Serialize + for<'de> Deserialize<'de>> From<Model<Box<T>>> for VecModel<T> {
    fn from(model: Model<Box<T>>) -> Self {
        VecModel(From::from(JsonPointer::from(model)))
    }
}
impl<T: Serialize + for<'de> Deserialize<'de>> From<JsonPointer> for VecModel<T> {
    fn from(ptr: JsonPointer) -> Self {
        VecModel(From::from(ptr))
    }
}
impl<T: Serialize + for<'de> Deserialize<'de>> HasModel for Vec<T> {
    type Model = VecModel<T>;
}

pub struct MapModel<T: Serialize + for<'de> Deserialize<'de> + for<'a> Index<&'a str>>(Model<T>);
impl<T: Serialize + for<'de> Deserialize<'de> + for<'a> Index<&'a str>> Deref for MapModel<T> {
    type Target = Model<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T> MapModel<T>
where
    T: Serialize + for<'de> Deserialize<'de> + for<'a> Index<&'a str>,
    for<'a, 'de> <T as Index<&'a str>>::Output: Serialize + Deserialize<'de>,
{
    pub fn idx(&self, idx: &str) -> Model<Option<<T as Index<&str>>::Output>> {
        self.child(idx)
    }
}
impl<T: Serialize + for<'de> Deserialize<'de> + for<'a> Index<&'a str>> From<Model<Box<T>>>
    for MapModel<T>
{
    fn from(model: Model<Box<T>>) -> Self {
        MapModel(From::from(JsonPointer::from(model)))
    }
}
impl<T: Serialize + for<'de> Deserialize<'de> + for<'a> Index<&'a str>> From<JsonPointer>
    for MapModel<T>
{
    fn from(ptr: JsonPointer) -> Self {
        MapModel(From::from(ptr))
    }
}
impl<K, V> HasModel for HashMap<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Hash + Eq,
    V: Serialize + for<'de> Deserialize<'de>,
    for<'a> HashMap<K, V>: Index<&'a str>,
{
    type Model = MapModel<HashMap<K, V>>;
}
impl<K, V> HasModel for BTreeMap<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Hash + Eq,
    V: Serialize + for<'de> Deserialize<'de>,
    for<'a> HashMap<K, V>: Index<&'a str>,
{
    type Model = MapModel<HashMap<K, V>>;
}
