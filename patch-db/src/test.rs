use super::*;
use crate as patch_db;
use patch_db_macro::HasModel;

#[tokio::test]
async fn basic() {
    let db = PatchDb::open("test.db").await.unwrap();
    db.put(
        &JsonPointer::<&'static str>::default(),
        &Sample {
            a: "test1".to_string(),
            b: Child {
                a: "test2".to_string(),
                b: 4,
            },
        },
    )
    .await
    .unwrap();
    let ptr: JsonPointer = "/b/b".parse().unwrap();
    db.put(&ptr, &"hello").await.unwrap();
    let get_res: Value = db.get(&ptr).await.unwrap();
    assert_eq!(get_res, "hello");
}

#[derive(Debug, serde::Deserialize, serde::Serialize, HasModel)]
pub struct Sample {
    a: String,
    #[model(name = ChildModel)]
    b: Child,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Child {
    a: String,
    b: usize,
}
