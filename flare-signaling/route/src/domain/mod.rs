//! 领域层模块
//!
//! 与 flare-storage 一致：model、repository（目录）、service、值对象等。

pub mod model;
pub mod repository;
pub mod service;
pub mod value_objects;

pub use model::*;
pub use repository::*;
pub use service::*;
pub use value_objects::*;
