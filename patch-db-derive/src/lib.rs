extern crate proc_macro;

use proc_macro::TokenStream;

#[proc_macro_derive(Model, attributes(serde))]
pub fn model_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast = syn::parse(input).unwrap();

    // Build the trait implementation
    patch_db_derive_internals::build_model(&ast).into()
}
