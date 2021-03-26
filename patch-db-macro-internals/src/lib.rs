use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Data, DataEnum, DataStruct, DeriveInput, Fields, Ident, Lit, LitStr, MetaNameValue, Type,
};

pub fn build_model(item: &DeriveInput) -> TokenStream {
    let mut model_name = None;
    for arg in item
        .attrs
        .iter()
        .filter(|attr| attr.path.is_ident("model"))
        .map(|attr| attr.parse_args::<MetaNameValue>().unwrap())
    {
        match arg {
            MetaNameValue {
                path,
                lit: Lit::Str(s),
                ..
            } if path.is_ident("name") => model_name = Some(s.parse().unwrap()),
            _ => (),
        }
    }
    match &item.data {
        Data::Struct(struct_ast) => build_model_struct(item, struct_ast, model_name),
        Data::Enum(enum_ast) => build_model_enum(item, enum_ast, model_name),
        _ => panic!("Models can only be created for Structs and Enums"),
    }
}

fn build_model_struct(
    base: &DeriveInput,
    ast: &DataStruct,
    model_name: Option<Ident>,
) -> TokenStream {
    let model_name = model_name.unwrap_or_else(|| {
        Ident::new(
            &format!("{}Model", base.ident),
            proc_macro2::Span::call_site(),
        )
    });
    let base_name = &base.ident;
    let model_vis = &base.vis;
    let mut child_fn_name: Vec<Ident> = Vec::new();
    let mut child_model: Vec<Type> = Vec::new();
    let mut child_path: Vec<LitStr> = Vec::new();
    let serde_rename_all = base
        .attrs
        .iter()
        .filter(|attr| attr.path.is_ident("serde"))
        .filter_map(|attr| attr.parse_args::<MetaNameValue>().ok())
        .filter(|nv| nv.path.is_ident("rename_all"))
        .find_map(|nv| match nv.lit {
            Lit::Str(s) => Some(s.value()),
            _ => None,
        });
    match &ast.fields {
        Fields::Named(f) => {
            for field in &f.named {
                let ident = field.ident.clone().unwrap();
                child_fn_name.push(ident.clone());
                let ty = &field.ty;
                child_model.push(syn::parse2(quote! { patch_db::Model<#ty> }).unwrap()); // TODO: check attr
                let serde_rename = field
                    .attrs
                    .iter()
                    .filter(|attr| attr.path.is_ident("serde"))
                    .filter_map(|attr| syn::parse2::<MetaNameValue>(attr.tokens.clone()).ok())
                    .filter(|nv| nv.path.is_ident("rename"))
                    .find_map(|nv| match nv.lit {
                        Lit::Str(s) => Some(s),
                        _ => None,
                    });
                match (serde_rename, serde_rename_all.as_ref().map(|s| s.as_str())) {
                    (Some(a), _) => child_path.push(a),
                    (None, Some("lowercase")) => child_path.push(LitStr::new(
                        &heck::CamelCase::to_camel_case(ident.to_string().as_str()).to_lowercase(),
                        ident.span(),
                    )),
                    (None, Some("UPPERCASE")) => child_path.push(LitStr::new(
                        &heck::CamelCase::to_camel_case(ident.to_string().as_str()).to_uppercase(),
                        ident.span(),
                    )),
                    (None, Some("PascalCase")) => child_path.push(LitStr::new(
                        &heck::CamelCase::to_camel_case(ident.to_string().as_str()),
                        ident.span(),
                    )),
                    (None, Some("camelCase")) => child_path.push(LitStr::new(
                        &heck::MixedCase::to_mixed_case(ident.to_string().as_str()),
                        ident.span(),
                    )),
                    (None, Some("SCREAMING_SNAKE_CASE")) => child_path.push(LitStr::new(
                        &heck::ShoutySnakeCase::to_shouty_snake_case(ident.to_string().as_str()),
                        ident.span(),
                    )),
                    (None, Some("kebab-case")) => child_path.push(LitStr::new(
                        &heck::KebabCase::to_kebab_case(ident.to_string().as_str()),
                        ident.span(),
                    )),
                    (None, Some("SCREAMING-KEBAB-CASE")) => child_path.push(LitStr::new(
                        &heck::ShoutyKebabCase::to_shouty_kebab_case(ident.to_string().as_str()),
                        ident.span(),
                    )),
                    _ => child_path.push(LitStr::new(&ident.to_string(), ident.span())),
                }
            }
        }
        Fields::Unnamed(f) => {
            if f.unnamed.len() == 1 {
            } else if f.unnamed.len() > 1 {
            }
            todo!()
        }
        Fields::Unit => (),
    }
    quote! {
        #[derive(Debug, Clone)]
        #model_vis struct #model_name(patch_db::Model<#base_name>);
        impl core::ops::Deref for #model_name {
            type Target = patch_db::Model<#base_name>;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
        impl #model_name {
            pub fn new(ptr: json_ptr::JsonPointer) -> Self {
                #model_name(patch_db::Model::new(ptr))
            }
            // foreach element, create accessor fn
            #(
                pub fn #child_fn_name(&self) -> #child_model {
                    self.0.child(#child_path).into()
                }
            )*
        }
        impl patch_db::HasModel for #base_name {
            type Model = #model_name;
        }
    }
}

fn build_model_enum(base: &DeriveInput, ast: &DataEnum, model_name: Option<Ident>) -> TokenStream {
    todo!()
}
