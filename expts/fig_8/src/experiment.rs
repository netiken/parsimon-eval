use std::{
    fmt::{self}, fs,
    path::{Path, PathBuf},
    time::Instant,
};

use parsimon::core::{
    network::{Flow, FlowId, Network, NodeId},
    units::{Bytes, Nanosecs},
};
use rayon::prelude::*;
use workload::{
    fabric::Cluster,
    flowgen::{FlowGenerator, StopWhen},
    spatial::SpatialData,
};

use crate::mix::{Mix, MixId};

use rustc_hash::{FxHashMap,FxHashSet};
use crate::ns3::Ns3Simulation;

const BASE_RTT: Nanosecs = Nanosecs::new(14_400);
const INIT_START_TIME: Nanosecs = Nanosecs::new(1_000_000_000);

#[derive(Debug, clap::Parser)]
pub struct Experiment {
    #[clap(long, default_value = "./data")]
    root: PathBuf,
    #[clap(long)]
    mixes: PathBuf,
    #[clap(long, default_value_t = 0)]
    seed: u64,
    #[clap(subcommand)]
    sim: SimKind,
    #[clap(long)]
    enable_app: bool,
    #[clap(long, default_value_t = 2000)]
    nr_flows: usize,
    #[clap(long, default_value_t = false)]
    enable_train: bool,
}

impl Experiment {
    pub fn run(&self) -> anyhow::Result<()> {
        let mixes: Vec<Mix> = serde_json::from_str(&fs::read_to_string(&self.mixes)?)?;

        // All ns3 simulations can run in parallel. Parsimon simulations are already massively
        // parallel, so they'll run one at a time to save memory.
        match self.sim {
            SimKind::Ns3 => {
                rayon::ThreadPoolBuilder::new()
                .num_threads(2)
                .build_global()
                .unwrap();
                mixes.par_iter().try_for_each(|mix| self.run_ns3(mix, false))?; 
            }
           
            SimKind::Mlsys => {
                for mix in &mixes {
                    self.run_ns3(mix, true)?;
                }
            }
            
        }
        Ok(())
    }

    fn run_ns3(&self, mix: &Mix, enable_mlsys: bool) -> anyhow::Result<()> {
        let sim = SimKind::Ns3;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let flows = self.flows(mix)?;
        let mut enable_tr = 0;
        let ns3_dir = if self.enable_train {
            enable_tr = 1;
            "../../../High-Precision-Congestion-Control/ns-3.39"
        } else {
            "../../../High-Precision-Congestion-Control/UNISON-for-ns-3"
        };

        // let start_read = Instant::now(); // timer start
        // construct SimNetwork
        let nodes = cluster.nodes().cloned().collect::<Vec<_>>();
        let links = cluster.links().cloned().collect::<Vec<_>>();
        let network = Network::new(&nodes, &links)?;
        let network = network.into_simulations_path(flows.clone());
        let (_channel_to_flowid_map, path_to_flowid_map): (
            &FxHashMap<(NodeId, NodeId), FxHashSet<FlowId>>,
            &FxHashMap<Vec<(NodeId, NodeId)>, FxHashSet<FlowId>>
        ) = match network.get_routes() {
            Some((channel_map, path_map)) => (channel_map, path_map),
            None => panic!("Routes not available"),
        };
        
        // Step 1: Create a new HashMap to store FlowId -> Path mapping
        let mut flowid_to_path_map: FxHashMap<FlowId, Vec<(NodeId, NodeId)>> = FxHashMap::default();
        for (path, flow_ids) in path_to_flowid_map.iter() {
            for flow_id in flow_ids {
                // Insert the flow ID and corresponding path into the reverse map
                flowid_to_path_map.insert(*flow_id, path.clone());
            }
        }
        
        let mut sorted_flowid_to_path_map: Vec<(&FlowId, &Vec<(NodeId, NodeId)>)> = flowid_to_path_map.iter().collect();
        sorted_flowid_to_path_map.sort_by_key(|&(flow_id, _)| flow_id);
        
        // Step 3: Write the sorted FlowId -> Path mapping into a file
        let mut results_str_flowid = String::new();
        for (flow_id, path) in sorted_flowid_to_path_map.iter() {
            results_str_flowid.push_str(&format!("{}:", flow_id));

            // Append the nodes in the path
            for (node_a, node_b) in path.iter() {
                results_str_flowid.push_str(&format!("{}-{}", node_a, node_b));
                results_str_flowid.push_str(",");
            }
            results_str_flowid.push_str("\n");
        }

        self.put_path_with_idx(
            mix,
            sim,
            1,
            format!(
                "{},{}\n{}",
                flowid_to_path_map.len(),
                path_to_flowid_map.len(),
                results_str_flowid
            ),
        )
        .unwrap();
        // let elapsed_read= start_read.elapsed().as_secs();
        // println!("read time-{}: {}", mix.id,elapsed_read);

        let start = Instant::now(); // timer start
        let ns3 = Ns3Simulation::builder()
            .ns3_dir(ns3_dir)
            .data_dir(self.sim_dir(mix, sim)?)
            .nodes(cluster.nodes().cloned().collect::<Vec<_>>())
            .links(cluster.links().cloned().collect::<Vec<_>>())
            .base_rtt(BASE_RTT)
            .flows(flows)
            .mix_id(mix.id)
            .bfsz(mix.bfsz)
            .window(Bytes::new(mix.window))
            .enable_pfc(mix.enable_pfc)
            .enable_tr(enable_tr)
            .cc_kind(mix.cc)
            .param_1(mix.param_1)
            .param_2(mix.param_2)
            .max_inflight_flows(mix.max_inflight_flows)
            .enable_mlsys(enable_mlsys)
            .build();
        let records = ns3
            .run()?
            .into_iter()
            .map(|rec| Record {
                mix_id: mix.id,
                flow_id: rec.id,
                size: rec.size,
                slowdown: rec.slowdown(),
                sim,
            })
            .collect::<Vec<_>>();
        self.put_records(mix, sim, &records)?;

        let elapsed_secs = start.elapsed().as_secs(); // timer end
        self.put_elapsed(mix, sim, elapsed_secs)?;
        Ok(())
    }

    fn flows(&self, mix: &Mix) -> anyhow::Result<Vec<Flow>> {
        let path = self.flow_file(mix)?;
        if !path.exists() {
            self.gen_flows(mix, &path)?;
        }
        let flows = parsimon::utils::read_flows(&path)?;
        Ok(flows)
    }

    fn gen_flows(&self, mix: &Mix, to: impl AsRef<Path>) -> anyhow::Result<()> {
        let spatial: SpatialData = serde_json::from_str(&fs::read_to_string(&mix.spatial)?)?;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let size_dist = utils::read_ecdf(&mix.size_dist)?;
        let flowgen = FlowGenerator::builder()
            .spatial_data(spatial)
            .cluster(cluster)
            .size_dist(size_dist)
            .lognorm_sigma(mix.lognorm_sigma)
            .max_load(mix.max_load)
            .stop_when(StopWhen::NrFlows(self.nr_flows))
            .seed(self.seed)
            .build();
        let mut flows = flowgen.generate();

        // Enable application-specific behavior if needed
        if self.enable_app{
            for flow in &mut flows {
                flow.start = INIT_START_TIME;
            }
        }
        let s = serde_json::to_string(&flows)?;
        fs::write(&to, s)?;
        Ok(())
    }

    fn put_records(&self, mix: &Mix, sim: SimKind, records: &[Record]) -> anyhow::Result<()> {
        let path = self.record_file(mix, sim)?;
        let mut wtr = csv::Writer::from_path(path)?;
        for record in records {
            wtr.serialize(record)?;
        }
        wtr.flush()?;
        Ok(())
    }

    fn put_elapsed(&self, mix: &Mix, sim: SimKind, secs: u64) -> anyhow::Result<()> {
        fs::write(self.elapsed_file(mix, sim)?, secs.to_string())?;
        Ok(())
    }

    fn put_path_with_idx(
        &self,
        mix: &Mix,
        sim: SimKind,
        path_idx: usize,
        path_str: String,
    ) -> anyhow::Result<()> {
        fs::write(self.path_file_with_idx(mix, sim, path_idx)?, path_str)?;
        Ok(())
    }

    fn mix_dir(&self, mix: &Mix) -> anyhow::Result<PathBuf> {
        let dir = [self.root.as_path(), mix.id.to_string().as_ref()]
            .into_iter()
            .collect();
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    fn sim_dir(&self, mix: &Mix, sim: SimKind) -> anyhow::Result<PathBuf> {
        let dir = [self.mix_dir(mix)?.as_path(), sim.to_string().as_ref()]
            .into_iter()
            .collect();
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    fn flow_file(&self, mix: &Mix) -> anyhow::Result<PathBuf> {
        let file = [self.mix_dir(mix)?.as_path(), "flows.json".as_ref()]
            .into_iter()
            .collect();
        Ok(file)
    }

    fn path_file_with_idx(
        &self,
        mix: &Mix,
        sim: SimKind,
        path_idx: usize,
    ) -> anyhow::Result<PathBuf> {
        let file = [
            self.sim_dir(mix, sim)?.as_path(),
            format!("path_{}.txt", path_idx).as_ref(),
        ]
        .into_iter()
        .collect();
        Ok(file)
    }

    fn record_file(&self, mix: &Mix, sim: SimKind) -> anyhow::Result<PathBuf> {
        let file = [self.sim_dir(mix, sim)?.as_path(), "records.csv".as_ref()]
            .into_iter()
            .collect();
        Ok(file)
    }

    fn elapsed_file(&self, mix: &Mix, sim: SimKind) -> anyhow::Result<PathBuf> {
        let file = [self.sim_dir(mix, sim)?.as_path(), "elapsed.txt".as_ref()]
            .into_iter()
            .collect();
        Ok(file)
    }

}

#[derive(Debug, Clone, Copy, clap::Subcommand, serde::Serialize, serde::Deserialize)]
pub enum SimKind {
    Ns3,
    Mlsys,
}

impl fmt::Display for SimKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            SimKind::Ns3 => "ns3",
            SimKind::Mlsys => "mlsys",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct Record {
    pub mix_id: MixId,
    pub flow_id: FlowId,
    pub size: Bytes,
    pub slowdown: f64,
    pub sim: SimKind,
}
