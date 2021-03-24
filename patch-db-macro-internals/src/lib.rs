use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    AttributeArgs, Fields, Ident, Item, ItemEnum, ItemStruct, Lit, LitStr, Meta, MetaNameValue,
    NestedMeta, Type,
};

pub fn build_model(attr: &AttributeArgs, item: &Item) -> TokenStream {
    let mut model_name = None;
    for arg in attr {
        match arg {
            NestedMeta::Meta(Meta::NameValue(MetaNameValue {
                path,
                lit: Lit::Str(s),
                ..
            })) if path.is_ident("name") => model_name = Some(s.parse().unwrap()),
            _ => (),
        }
    }
    match item {
        Item::Struct(struct_ast) => build_model_struct(struct_ast, model_name),
        Item::Enum(enum_ast) => build_model_enum(enum_ast, model_name),
        _ => panic!("Models can only be created for Structs and Enums"),
    }
}

fn build_model_struct(ast: &ItemStruct, model_name: Option<Ident>) -> TokenStream {
    let model_name = model_name.unwrap_or_else(|| {
        Ident::new(
            &format!("{}Model", ast.ident),
            proc_macro2::Span::call_site(),
        )
    });
    let base_name = &ast.ident;
    let model_vis = &ast.vis;
    let mut child_fn_name: Vec<Ident> = Vec::new();
    let mut child_model: Vec<Type> = Vec::new();
    let mut child_path: Vec<LitStr> = Vec::new();
    let serde_rename_all = todo!();
    match &ast.fields {
        Fields::Named(f) => {
            for field in &f.named {
                let ident = field.ident.clone().unwrap();
                child_fn_name.push(ident.clone());
                child_model.push(field.ty.clone());
                let serde_rename = todo!();
                match (serde_rename, serde_rename_all) {
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
        Fields::Unnamed(f) => {}
        Fields::Unit => (),
    }
    quote! {
        #model_vis struct #model_name(patch_db::Model<#base_name>);
        impl core::ops::Deref for #model_name {
            type Target = patch_db::Model<#base_name>;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
        impl #model_name {
            // foreach element, create accessor fn
            #(
                pub fn #child_fn_name(&self) -> #child_model {
                    self.0.child(#child_path).into()
                }
            )*
        }
    }
}

fn build_model_enum(ast: &ItemEnum, model_name: Option<Ident>) -> TokenStream {
    todo!()
}
