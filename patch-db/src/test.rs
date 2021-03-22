use super::*;

#[tokio::test]
async fn basic() {
    let db = PatchDb::open("test.db").await.unwrap();
    let _rev = db
        .put(&JsonPointer::<&'static str>::default(), &"test")
        .await
        .unwrap();
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Sample {
    a: String,
    b: Child,
}

pub struct SampleModel(Model<Sample>);

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Child {
    a: String,
    b: usize,
}

pub struct ChildModel(Model<Child>);
