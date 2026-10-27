//! Byte-exact comparison of two artifact trees, plus a blake3 manifest.
//!
//! The only tolerated difference is the documented known exception (design §4/§7): HTTP
//! records whose label starts with `known_exception:` (requests carrying with_evidence=true)
//! may differ if the new build answers them with status 400. They are always printed.
use anyhow::{Context, Result, bail};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

fn files(root: &Path) -> Result<BTreeSet<PathBuf>> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeSet<PathBuf>) -> Result<()> {
        for entry in std::fs::read_dir(dir).with_context(|| dir.display().to_string())? {
            let path = entry?.path();
            if path.is_dir() {
                walk(root, &path, out)?;
            } else {
                out.insert(path.strip_prefix(root)?.to_owned());
            }
        }
        Ok(())
    }
    let mut out = BTreeSet::new();
    walk(root, root, &mut out)?;
    Ok(out)
}
pub fn manifest(root: &Path) -> Result<String> {
    let mut text = String::new();
    for path in files(root)? {
        if path.as_os_str() == "MANIFEST.blake3" {
            continue;
        }
        let bytes = std::fs::read(root.join(&path))?;
        text.push_str(&format!(
            "{}  {}\n",
            blake3::hash(&bytes).to_hex(),
            path.display()
        ));
    }
    let total = blake3::hash(text.as_bytes()).to_hex();
    Ok(format!("{text}{total}  TOTAL\n"))
}
fn known_exception(line: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(line)
        .ok()
        .and_then(|v| {
            v["label"]
                .as_str()
                .map(|l| l.starts_with("known_exception:"))
        })
        .unwrap_or(false)
}
pub fn compare(expected: &Path, actual: &Path) -> Result<()> {
    let (left, right) = (files(expected)?, files(actual)?);
    let mut failures = 0;
    for path in left.symmetric_difference(&right) {
        if path.as_os_str() == "MANIFEST.blake3" {
            continue;
        }
        failures += 1;
        println!(
            "DIFF {}: only in {}",
            path.display(),
            if left.contains(path) {
                "expected"
            } else {
                "actual"
            }
        );
    }
    for path in left.intersection(&right) {
        if path.as_os_str() == "MANIFEST.blake3" {
            continue;
        }
        let a = std::fs::read(expected.join(path))?;
        let b = std::fs::read(actual.join(path))?;
        if a == b {
            continue;
        }
        let (ta, tb) = (String::from_utf8_lossy(&a), String::from_utf8_lossy(&b));
        let (la, lb): (Vec<_>, Vec<_>) = (ta.lines().collect(), tb.lines().collect());
        let http = path.file_name().is_some_and(|n| n == "http.jsonl");
        let mut file_failed = la.len() != lb.len() || path.extension().is_none_or(|e| e != "jsonl");
        for (n, (x, y)) in la.iter().zip(&lb).enumerate() {
            if x == y {
                continue;
            }
            let parse = |l: &str| serde_json::from_str::<serde_json::Value>(l).unwrap_or_default();
            let (vx, vy) = (parse(x), parse(y));
            let same_request = vx["label"] == vy["label"]
                && vx["uri"] == vy["uri"]
                && vx["t"] == vy["t"]
                && vx["request_body"] == vy["request_body"];
            if http && known_exception(x) && same_request && vy["status"] == 400 {
                println!(
                    "KNOWN_EXCEPTION {} line {}:\n  0.4.24: {x}\n  actual: {y}",
                    path.display(),
                    n + 1
                );
                continue;
            }
            file_failed = true;
            println!(
                "DIFF {} line {}:\n  expected: {x}\n  actual:   {y}",
                path.display(),
                n + 1
            );
        }
        if file_failed {
            failures += 1;
            println!("DIFF {}: content differs", path.display());
        }
    }
    if failures > 0 {
        bail!("{failures} artifact(s) differ");
    }
    println!("IDENTICAL (apart from any KNOWN_EXCEPTION lines listed above)");
    Ok(())
}
