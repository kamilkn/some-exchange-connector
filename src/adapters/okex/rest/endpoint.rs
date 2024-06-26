use http::Method;

use crate::ports::endpoint::Endpoint;
use crate::adapters::okex::rest::response::OkexResponse;
use crate::adapters::okex::rest::model::OkexOrderBookSnapshot;
use crate::adapters::okex::rest::request::GetOrderBookRequest;

pub struct GetOrderBook;

impl Endpoint for GetOrderBook {
    type Request = GetOrderBookRequest;
    type Response = OkexResponse<OkexOrderBookSnapshot>;

    const METHOD: Method = Method::GET;
    const PATH: &'static str = "/api/v5/market/books";
}
