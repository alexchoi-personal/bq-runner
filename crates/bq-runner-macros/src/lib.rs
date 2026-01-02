use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, FnArg, ItemFn, Pat, ReturnType};

#[proc_macro_attribute]
pub fn rpc(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let attr_str = attr.to_string();

    let fn_name = &input.sig.ident;
    let block = &input.block;
    let output = &input.sig.output;

    let return_type = match output {
        ReturnType::Type(_, ty) => ty,
        ReturnType::Default => {
            return syn::Error::new_spanned(output, "RPC methods must have a return type")
                .to_compile_error()
                .into();
        }
    };

    let has_session = attr_str.contains("session");

    if has_session {
        let (param_name, param_type) = match input.sig.inputs.first() {
            Some(FnArg::Typed(pat_type)) => {
                let name = match pat_type.pat.as_ref() {
                    Pat::Ident(ident) => &ident.ident,
                    _ => {
                        return syn::Error::new_spanned(&pat_type.pat, "Expected identifier")
                            .to_compile_error()
                            .into();
                    }
                };
                let ty = &pat_type.ty;
                (name, ty)
            }
            _ => {
                return syn::Error::new_spanned(
                    &input.sig,
                    "Session RPC methods must have a parameter",
                )
                .to_compile_error()
                .into();
            }
        };

        let expanded = quote! {
            async fn #fn_name(&self, params: serde_json::Value) -> crate::error::Result<serde_json::Value> {
                let #param_name: #param_type = serde_json::from_value(params)?;
                let session_id = parse_uuid(&#param_name.session_id)?;
                let sm = &self.session_manager;
                let __result: #return_type = #block;
                Ok(serde_json::json!(__result))
            }
        };

        expanded.into()
    } else {
        let expanded = quote! {
            async fn #fn_name(&self, _params: serde_json::Value) -> crate::error::Result<serde_json::Value> {
                let __result: #return_type = #block;
                Ok(serde_json::json!(__result))
            }
        };

        expanded.into()
    }
}
