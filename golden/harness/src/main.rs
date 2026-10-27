//! kp-golden: closed-mode runtime differential golden harness for kline-proxy-rs.
//!
//!   kp-golden list
//!   kp-golden run <scenario> <out_dir> <work_dir>
//!   kp-golden hash <dir>                 (blake3 manifest)
//!   kp-golden compare <expected> <actual>
mod compare;
mod control;
mod mock;
mod scenarios;
mod sim;
use anyhow::{Context, Result, bail};
use std::{path::PathBuf, sync::Arc, time::Duration};

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["list"] => {
            for (name, _, _) in scenarios::ALL {
                println!("{name}");
            }
            Ok(())
        }
        ["run", name, out, work] => run(name, PathBuf::from(out), PathBuf::from(work)),
        ["hash", dir] => {
            print!("{}", compare::manifest(std::path::Path::new(dir))?);
            Ok(())
        }
        ["compare", expected, actual] => {
            compare::compare(std::path::Path::new(expected), std::path::Path::new(actual))
        }
        _ => bail!(
            "usage: kp-golden list | run <scenario> <out> <work> | hash <dir> | compare <expected> <actual>"
        ),
    }
}

fn run(name: &str, out: PathBuf, work: PathBuf) -> Result<()> {
    let (_, base_ms, scenario) = scenarios::ALL
        .iter()
        .find(|(n, _, _)| *n == name)
        .with_context(|| format!("unknown scenario {name}"))?;
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work)?;
    let control = Arc::new(control::Control::default());
    control::install_tracing(control.clone());
    control::watchdog(control.clone(), Duration::from_secs(600));
    let (park, unpark) = (control.clone(), control.clone());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .on_thread_park(move || park.before_park())
        .on_thread_unpark(move || unpark.after_unpark())
        .build()?;
    let result = runtime.block_on(async {
        control.hold_now();
        let clock = Arc::new(control::VirtualClock::new(*base_ms));
        clock.start();
        control.set_clock(clock.clone());
        let plan = scenarios::default_plan();
        let mut sim = sim::Sim::new(control.clone(), clock, plan, work.clone()).await;
        scenario(&mut sim).await?;
        if sim.inst.is_some() {
            sim.shutdown().await;
        }
        let violations = control.violations();
        if !violations.is_empty() {
            bail!("determinism invariant violated: {violations:?}");
        }
        sim.write(&out)?;
        anyhow::Ok(())
    });
    control.finish();
    drop(runtime);
    result
}
