use super::*;
use crate as patch_db;
use patch_db_macro::HasModel;
use proptest::prelude::*;
use std::future::Future;
use tokio::{fs, runtime::Builder};

async fn init_db(db_name: String) -> PatchDb {
    cleanup_db(&db_name).await;
    let db = PatchDb::open(db_name).await.unwrap();
    db.put(
        &JsonPointer::<&'static str>::default(),
        &Sample {
            a: "test1".to_string(),
            b: Child {
                a: "test2".to_string(),
                b: 1,
            },
        },
    )
    .await
    .unwrap();
    db
}

async fn cleanup_db(db_name: &str) {
    fs::remove_file(db_name).await.ok();
}

async fn put_string_into_root(db: PatchDb, s: String) -> Arc<Revision> {
    db.put(&JsonPointer::<&'static str>::default(), &s)
        .await
        .unwrap()
}

#[tokio::test]
async fn basic() {
    let db = init_db("test.db".to_string()).await;
    let ptr: JsonPointer = "/b/b".parse().unwrap();
    let mut get_res: Value = db.get(&ptr).await.unwrap();
    assert_eq!(get_res, 1);
    db.put(&ptr, "hello").await.unwrap();
    get_res = db.get(&ptr).await.unwrap();
    assert_eq!(get_res, "hello");
    cleanup_db("test.db").await;
}

#[tokio::test]
async fn transaction() {
    let db = init_db("test.db".to_string()).await;
    let mut tx = db.begin();
    let ptr: JsonPointer = "/b/b".parse().unwrap();
    tx.put(&ptr, &(2 as usize)).await.unwrap();
    tx.put(&ptr, &(1 as usize)).await.unwrap();
    let _res = tx.commit().await.unwrap();
    println!("res = {:?}", _res);
    cleanup_db("test.db").await;
}

fn run_future<S: Into<String>, Fut: Future<Output = ()>>(name: S, fut: Fut) {
    Builder::new_multi_thread()
        .thread_name(name)
        .build()
        .unwrap()
        .block_on(fut)
}

proptest! {
    #[test]
    fn doesnt_crash(s in "\\PC*") {
        run_future("test-doesnt-crash", async {
            let db = init_db("test.db".to_string()).await;
            put_string_into_root(db, s).await;
            cleanup_db(&"test.db".to_string()).await;
        });
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize, HasModel)]
pub struct Sample {
    a: String,
    #[model(name = "ChildModel")]
    b: Child,
}

#[derive(Debug, serde::Deserialize, serde::Serialize, HasModel)]
pub struct Child {
    a: String,
    b: usize,
}
