mod admin_auth_middleware;
mod admin_contract;
mod admin_extract;
mod admin_handler;
mod audit_target;
mod hooks_handler;
mod media_handler;
mod ops_handler;
mod router;

pub use admin_auth_middleware::{AdminRoute, VerifiedStaffContext};
pub use admin_contract::build_admin_capabilities;
pub use ops_handler::AdminCapabilityDownstream;
pub use router::{admin_openapi, create_admin_router};
