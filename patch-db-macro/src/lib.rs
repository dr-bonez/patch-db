extern crate proc_macro;

use proc_macro::TokenStream;
use syn::{parse_macro_input, AttributeArgs, Item};

#[proc_macro_attribute]
pub fn model(attr_in: TokenStream, item_in: TokenStream) -> TokenStream {
    let mut res = proc_macro2::TokenStream::from(item_in.clone());
    let attr = parse_macro_input!(attr_in as AttributeArgs);
    let item = parse_macro_input!(item_in as Item);
    res.extend(patch_db_macro_internals::build_model(&attr, &item));
    res.into()
}
