//! Aggregated deployment profile planning for Flare IM Core.
//!
//! This crate is the first step toward "compile-time monolith, runtime split"
//! deployment. It deliberately models process shapes before embedding runners:
//! existing service bootstraps still own their process signal handling, so the
//! next step is to add shutdown-injected embedded runners behind this contract.

#![forbid(unsafe_code)]

pub mod embedded;

use flare_im_contracts::service_names::{
    ACCESS_GATEWAY, ADMIN_GATEWAY, API_GATEWAY, CAPABILITY, CONVERSATION, MEDIA, MESSAGE_INGEST,
    ORCHESTRATOR, PUSH_PROXY, PUSH_SERVER, PUSH_WORKER, SIGNALING_ONLINE, SIGNALING_ROUTE,
    STORAGE_READER, STORAGE_WRITER, SYNC_ORCHESTRATOR,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentProfile {
    Dev,
    Standard,
    Full,
}

impl DeploymentProfile {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "dev" | "development" => Some(Self::Dev),
            "standard" | "std" => Some(Self::Standard),
            "full" | "microservices" => Some(Self::Full),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dev => "dev",
            Self::Standard => "standard",
            Self::Full => "full",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StandardGroup {
    Edge,
    Core,
    Data,
}

impl StandardGroup {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "edge" => Some(Self::Edge),
            "core" => Some(Self::Core),
            "data" => Some(Self::Data),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Edge => "edge",
            Self::Core => "core",
            Self::Data => "data",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessShape {
    EmbeddedSingleProcess,
    EmbeddedServiceGroup,
    IndependentServiceProcess,
}

impl ProcessShape {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EmbeddedSingleProcess => "embedded-single-process",
            Self::EmbeddedServiceGroup => "embedded-service-group",
            Self::IndependentServiceProcess => "independent-service-process",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddedReadiness {
    BootstrapEntrypointExposed,
    NeedsShutdownAdapter,
}

impl EmbeddedReadiness {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BootstrapEntrypointExposed => "bootstrap-entrypoint-exposed",
            Self::NeedsShutdownAdapter => "needs-shutdown-adapter",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServiceSpec {
    pub service_name: &'static str,
    pub package: &'static str,
    pub binary: &'static str,
    pub workspace_member: &'static str,
    pub group: StandardGroup,
    pub embedded_readiness: EmbeddedReadiness,
}

impl ServiceSpec {
    pub const fn new(
        service_name: &'static str,
        package: &'static str,
        binary: &'static str,
        workspace_member: &'static str,
        group: StandardGroup,
    ) -> Self {
        Self {
            service_name,
            package,
            binary,
            workspace_member,
            group,
            embedded_readiness: EmbeddedReadiness::BootstrapEntrypointExposed,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeUnit {
    pub name: String,
    pub profile: DeploymentProfile,
    pub group: Option<StandardGroup>,
    pub shape: ProcessShape,
    pub services: Vec<ServiceSpec>,
}

impl RuntimeUnit {
    pub fn package_commands(&self) -> Vec<String> {
        self.services
            .iter()
            .map(|service| format!("cargo run -p {} --bin {}", service.package, service.binary))
            .collect()
    }
}

pub const ALL_RUNTIME_SERVICES: [ServiceSpec; 16] = [
    ServiceSpec::new(
        API_GATEWAY,
        "flare-api-gateway",
        "flare-api-gateway",
        "flare-api-gateway",
        StandardGroup::Edge,
    ),
    ServiceSpec::new(
        ADMIN_GATEWAY,
        "flare-admin-gateway",
        "flare-admin-gateway",
        "flare-admin-gateway",
        StandardGroup::Edge,
    ),
    ServiceSpec::new(
        ACCESS_GATEWAY,
        "flare-signaling-gateway",
        "flare-signaling-gateway",
        "flare-signaling/gateway",
        StandardGroup::Edge,
    ),
    ServiceSpec::new(
        SIGNALING_ROUTE,
        "flare-signaling-route",
        "flare-signaling-route",
        "flare-signaling/route",
        StandardGroup::Edge,
    ),
    ServiceSpec::new(
        MESSAGE_INGEST,
        "flare-message-ingest",
        "flare-message-ingest",
        "flare-message-ingest",
        StandardGroup::Core,
    ),
    ServiceSpec::new(
        ORCHESTRATOR,
        "flare-orchestrator",
        "flare-orchestrator",
        "flare-orchestrator",
        StandardGroup::Core,
    ),
    ServiceSpec::new(
        CONVERSATION,
        "flare-conversation",
        "flare-conversation",
        "flare-conversation",
        StandardGroup::Core,
    ),
    ServiceSpec::new(
        SYNC_ORCHESTRATOR,
        "flare-sync-orchestrator",
        "flare-sync-orchestrator",
        "flare-sync-orchestrator",
        StandardGroup::Core,
    ),
    ServiceSpec::new(
        PUSH_PROXY,
        "flare-push-proxy",
        "flare-push-proxy",
        "flare-push/proxy",
        StandardGroup::Core,
    ),
    ServiceSpec::new(
        PUSH_SERVER,
        "flare-push-server",
        "flare-push-server",
        "flare-push/server",
        StandardGroup::Core,
    ),
    ServiceSpec::new(
        PUSH_WORKER,
        "flare-push-worker",
        "flare-push-worker",
        "flare-push/worker",
        StandardGroup::Core,
    ),
    ServiceSpec::new(
        CAPABILITY,
        "flare-capability",
        "flare-capability",
        "flare-capability",
        StandardGroup::Core,
    ),
    ServiceSpec::new(
        MEDIA,
        "flare-media",
        "flare-media",
        "flare-media",
        StandardGroup::Core,
    ),
    ServiceSpec::new(
        STORAGE_WRITER,
        "flare-storage-writer",
        "flare-storage-writer",
        "flare-storage/writer",
        StandardGroup::Data,
    ),
    ServiceSpec::new(
        STORAGE_READER,
        "flare-storage-reader",
        "flare-storage-reader",
        "flare-storage/reader",
        StandardGroup::Data,
    ),
    ServiceSpec::new(
        SIGNALING_ONLINE,
        "flare-signaling-online",
        "flare-signaling-online",
        "flare-signaling/online",
        StandardGroup::Data,
    ),
];

pub fn profile_units(profile: DeploymentProfile) -> Vec<RuntimeUnit> {
    match profile {
        DeploymentProfile::Dev => vec![RuntimeUnit {
            name: "flare-im-all-dev".to_string(),
            profile,
            group: None,
            shape: ProcessShape::EmbeddedSingleProcess,
            services: ALL_RUNTIME_SERVICES.to_vec(),
        }],
        DeploymentProfile::Standard => [
            StandardGroup::Edge,
            StandardGroup::Core,
            StandardGroup::Data,
        ]
        .into_iter()
        .map(standard_group_unit)
        .collect(),
        DeploymentProfile::Full => ALL_RUNTIME_SERVICES
            .iter()
            .copied()
            .map(|service| RuntimeUnit {
                name: service.binary.to_string(),
                profile,
                group: Some(service.group),
                shape: ProcessShape::IndependentServiceProcess,
                services: vec![service],
            })
            .collect(),
    }
}

pub fn standard_group_unit(group: StandardGroup) -> RuntimeUnit {
    RuntimeUnit {
        name: format!("flare-im-all-{}", group.as_str()),
        profile: DeploymentProfile::Standard,
        group: Some(group),
        shape: ProcessShape::EmbeddedServiceGroup,
        services: ALL_RUNTIME_SERVICES
            .iter()
            .copied()
            .filter(|service| service.group == group)
            .collect(),
    }
}

pub fn find_service(service_name: &str) -> Option<ServiceSpec> {
    ALL_RUNTIME_SERVICES
        .iter()
        .copied()
        .find(|service| service.service_name == service_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dev_profile_models_all_runtime_services_as_one_unit() {
        let units = profile_units(DeploymentProfile::Dev);

        assert_eq!(units.len(), 1);
        assert_eq!(units[0].shape, ProcessShape::EmbeddedSingleProcess);
        assert_eq!(units[0].services.len(), 16);
    }

    #[test]
    fn standard_profile_models_three_operational_groups() {
        let units = profile_units(DeploymentProfile::Standard);

        assert_eq!(units.len(), 3);
        assert_eq!(standard_group_unit(StandardGroup::Edge).services.len(), 4);
        assert_eq!(standard_group_unit(StandardGroup::Core).services.len(), 9);
        assert_eq!(standard_group_unit(StandardGroup::Data).services.len(), 3);
    }

    #[test]
    fn full_profile_keeps_each_service_independent() {
        let units = profile_units(DeploymentProfile::Full);

        assert_eq!(units.len(), ALL_RUNTIME_SERVICES.len());
        assert!(units.iter().all(|unit| unit.services.len() == 1));
        assert!(
            units
                .iter()
                .all(|unit| unit.shape == ProcessShape::IndependentServiceProcess)
        );
    }

    #[test]
    fn profile_uses_contract_service_names() {
        assert!(find_service(API_GATEWAY).is_some());
        assert!(find_service(ACCESS_GATEWAY).is_some());
        assert!(find_service(CAPABILITY).is_some());
        assert!(find_service(STORAGE_WRITER).is_some());
    }
}
