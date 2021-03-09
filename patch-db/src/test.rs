use super::*;

#[tokio::test]
async fn basic() {
    let db = PatchDb::open("test.db").await.unwrap();
    let _rev = db
        .put(&JsonPointer::<&'static str>::default(), &"test")
        .await
        .unwrap();
}
