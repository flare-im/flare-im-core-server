use flare_im_all::{
    DeploymentProfile, RuntimeUnit, StandardGroup, profile_units, standard_group_unit,
};

fn main() {
    let mut args = std::env::args().skip(1);
    let Some(first) = args.next() else {
        print_plan(
            DeploymentProfile::Dev,
            profile_units(DeploymentProfile::Dev),
        );
        return;
    };

    if matches!(first.as_str(), "-h" | "--help" | "help") {
        print_usage();
        return;
    }

    let Some(profile) = DeploymentProfile::parse(&first) else {
        eprintln!("unknown deployment profile: {first}");
        print_usage();
        std::process::exit(2);
    };

    let units = if profile == DeploymentProfile::Standard {
        match args.next().as_deref().and_then(StandardGroup::parse) {
            Some(group) => vec![standard_group_unit(group)],
            None => profile_units(profile),
        }
    } else {
        profile_units(profile)
    };

    print_plan(profile, units);
}

fn print_usage() {
    println!("Usage: flare-im-all [dev|standard|full] [edge|core|data]");
    println!("Default: flare-im-all dev");
    println!();
    println!("This first-stage binary prints the deployment profile plan.");
    println!(
        "Embedded service runners will be enabled after each service accepts an injected shutdown signal."
    );
}

fn print_plan(profile: DeploymentProfile, units: Vec<RuntimeUnit>) {
    println!("Flare IM deployment profile: {}", profile.as_str());
    for unit in units {
        println!();
        println!("process: {}", unit.name);
        println!("shape: {}", unit.shape.as_str());
        if let Some(group) = unit.group {
            println!("group: {}", group.as_str());
        }
        for service in unit.services {
            println!(
                "  - {} package={} bin={} readiness={}",
                service.service_name,
                service.package,
                service.binary,
                service.embedded_readiness.as_str()
            );
        }
    }
}
