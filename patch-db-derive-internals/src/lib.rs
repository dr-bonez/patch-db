use proc_macro2::TokenStream;
use quote::quote;

pub fn build_model(input: &syn::DeriveInput) -> TokenStream {
    match &input.data {
        syn::Data::Struct(struct_ast) => build_model_struct(input, struct_ast),
        syn::Data::Enum(enum_ast) => build_model_enum(enum_ast),
        syn::Data::Union(_) => panic!("Unions are not supported"),
    }
}

fn build_model_struct(input: &syn::DeriveInput, ast: &syn::DataStruct) -> TokenStream {
    let model_name = syn::Ident::new(
        &format!("{}Model", input.ident),
        proc_macro2::Span::call_site(),
    );
    let base_name = &input.ident;
    let model_vis = &input.vis;
    quote! {
        #model_vis struct #model_name<Tx: patch_db::Checkpoint> {
            data: Option<Box<#base_name>>,
            ptr: json_ptr::JsonPointer,
            tx: Tx,
        }
        impl<Tx: patch_db::Checkpoint> #model_name<Tx> {
            pub fn get(&mut self, lock: patch_db::LockType) -> Result<&#base_name, patch_db::Error> {
                if let Some(data) = self.data.as_ref() {
                    match lock {
                        patch_db::LockType::None => Ok(data),

                    }
                } else {
                    self.tx.get(&self.ptr, lock)
                }
            }
        }
    }
}

fn build_model_enum(ast: &syn::DataEnum) -> TokenStream {
    todo!()
}
