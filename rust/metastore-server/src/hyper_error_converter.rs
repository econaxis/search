use hyper::{Body, Response};

pub fn map_str_error(a: Result<Response<Body>, String>) -> Result<Response<Body>, String> {
    if let Err(err) = a {
        let errorstr = format!("Error!: {}", err);
        Ok(Response::new(errorstr.into()))
    } else {
        a
    }
}
