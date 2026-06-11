use flare_im_all::{
    DeploymentProfile, RuntimeUnit, StandardGroup, profile_units, standard_group_unit,
};

#[tokio::main(flavor = "current_thread")]
async fn main() {
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

    if first == "run" {
        run_command(args.collect()).await;
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
    println!("       flare-im-all run dev");
    println!("       flare-im-all run standard <edge|core|data>");
    println!("Default: flare-im-all dev");
    println!();
    println!("Without `run`, the command prints the deployment profile plan.");
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

async fn run_command(args: Vec<String>) {
    let Some(profile_arg) = args.first() else {
        eprintln!("missing profile for `run`");
        print_usage();
        std::process::exit(2);
    };

    let Some(profile) = DeploymentProfile::parse(profile_arg) else {
        eprintln!("unknown deployment profile: {profile_arg}");
        print_usage();
        std::process::exit(2);
    };

    let result = match profile {
        DeploymentProfile::Dev => flare_im_all::embedded::run_embedded_dev().await,
        DeploymentProfile::Standard => {
            let Some(group_arg) = args.get(1) else {
                eprintln!("`run standard` requires one of: edge, core, data");
                print_usage();
                std::process::exit(2);
            };
            let Some(group) = StandardGroup::parse(group_arg) else {
                eprintln!("unknown standard group: {group_arg}");
                print_usage();
                std::process::exit(2);
            };
            flare_im_all::embedded::run_embedded_standard_group(group).await
        }
        DeploymentProfile::Full => Err(flare_server_core::error::FlareError::system(
            "full profile uses independent service processes; run each service binary directly",
        )),
    };

    if let Err(error) = result {
        eprintln!("flare-im-all run failed: {error}");
        std::process::exit(1);
    }
}
