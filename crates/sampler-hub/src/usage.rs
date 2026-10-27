//cpu and memory of process trees, measured on demand
use std::collections::HashMap;
use std::sync::Mutex;

use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Load {
    //share of the whole machine, 0 to 100
    pub cpu: f32,
    pub memory: u64,
}

pub struct Usage {
    system: Mutex<System>,
    cores: f32,
}

impl std::fmt::Debug for Usage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Usage")
    }
}

impl Default for Usage {
    fn default() -> Self {
        Self {
            system: Mutex::new(System::new()),
            cores: std::thread::available_parallelism().map_or(1, |n| n.get()) as f32,
        }
    }
}

impl Usage {
    //cpu is averaged since the previous call
    pub fn measure(&self, roots: &[u32]) -> Vec<Load> {
        let mut system = self.system.lock().unwrap();
        system.refresh_processes_specifics(
            ProcessesToUpdate::All,
            true,
            ProcessRefreshKind::nothing().with_cpu().with_memory(),
        );
        let mut children: HashMap<Pid, Vec<Pid>> = HashMap::new();
        for (pid, process) in system.processes() {
            if let Some(parent) = process.parent() {
                children.entry(parent).or_default().push(*pid);
            }
        }
        roots
            .iter()
            .map(|root| {
                let mut load = Load::default();
                let mut stack = vec![Pid::from_u32(*root)];
                while let Some(pid) = stack.pop() {
                    if let Some(process) = system.process(pid) {
                        load.cpu += process.cpu_usage() / self.cores;
                        load.memory += process.memory();
                    }
                    stack.extend(children.get(&pid).into_iter().flatten());
                }
                load
            })
            .collect()
    }

    //the hub with llama.cpp and every mcp server
    pub fn hub(&self) -> Load {
        self.measure(&[std::process::id()])[0]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_hub_tree_uses_memory() {
        let usage = Usage::default();
        let load = usage.hub();
        assert!(load.memory > 0);
        assert!(
            usage.measure(&[u32::MAX])[0] == Load::default(),
            "a missing process weighs nothing"
        );
    }
}
