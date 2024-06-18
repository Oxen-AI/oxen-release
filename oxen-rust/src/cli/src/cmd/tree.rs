use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::api;
use liboxen::core::index::commit_merkle_tree::CommitMerkleTree;
use liboxen::core::index::ObjectDBReader;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use std::path::Path;
use std::time::Instant;

use crate::cmd::RunCmd;
pub const NAME: &str = "tree";
pub struct TreeCmd;

#[async_trait]
impl RunCmd for TreeCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Print the merkle tree 🌲 of a commit.")
            .arg(
                Arg::new("commit")
                    .long("commit")
                    .short('c')
                    .help("The commit to print the tree of.")
                    .default_value("HEAD")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("path")
                    .long("path")
                    .short('p')
                    .help("The path to print the tree of.")
                    .default_value("")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("depth")
                    .long("depth")
                    .short('d')
                    .help("How many levels deep to traverse the tree. -1 for all.")
                    .default_value("-1")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("old")
                    .long("old")
                    .help("To use the old lookup method")
                    .action(clap::ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let depth = args
            .get_one::<String>("depth")
            .expect("Must supply depth")
            .parse::<i32>()
            .expect("depth must be a valid integer.");
        let commit_id = args
            .get_one::<String>("commit")
            .expect("Must supply commit");
        let repo = LocalRepository::from_current_dir()?;

        let commit = if commit_id == "HEAD" {
            api::local::commits::head_commit(&repo)?
        } else {
            let Some(commit) = api::local::commits::get_by_id(&repo, commit_id)? else {
                return Err(OxenError::basic_str(format!(
                    "Commit {} not found",
                    commit_id
                )));
            };
            commit
        };

        let path = args.get_one::<String>("path").expect("Must supply path");

        if args.get_flag("old") {
            println!("Run old!");
            let load_start = Instant::now(); // Start timing

            // let page = 1;
            // let page_size = 100;
            // let (paginated_entries, _dir) = api::local::entries::list_directory(
            //     &repo,
            //     &commit,
            //     &Path::new(path),
            //     &commit.id,
            //     page,
            //     page_size,
            // )?;
            // println!("Got {:?} entries", paginated_entries.entries.len());

            let path = Path::new(path);
            let filename = path.file_name().unwrap().to_str().unwrap();
            let parent = path.parent().unwrap();
            let object_reader = ObjectDBReader::new(&repo)?;
            let cder = liboxen::core::index::CommitDirEntryReader::new(
                &repo,
                &commit.id,
                parent,
                object_reader.clone(),
            )?;
            println!("looking up entry {}", filename);
            let entry = cder.get_entry(filename)?;
            println!("Got entry {:?}", entry);

            let load_duration = load_start.elapsed(); // Calculate duration
            println!("Time to load tree: {:?}", load_duration);

            return Ok(());
        }

        let load_start = Instant::now(); // Start timing
        let root = CommitMerkleTree::read_path(&repo, &commit, path.as_str())?;

        // List directories in the .oxen/tree dir
        /*type TreeNode = HashMap<u128, MerkleNode>;
        let mut data: Vec<TreeNode> = vec![];
        let path = Path::new(&repo.path).join(".oxen").join("tree");
        let mut total_open_duration = Duration::new(0, 0);
        let mut total_map_duration = Duration::new(0, 0);
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                println!("Opening tree at {:?}", path);
                let open_start = Instant::now(); // Start timing
                let mut tree_db = MerkleNodeDB::open(&path, true)?;
                let open_duration = open_start.elapsed(); // Calculate duration
                println!("Time to open tree: {:?}", open_duration);
                total_open_duration += open_duration;
                let map_start = Instant::now(); // Start timing
                let vals: TreeNode = tree_db.map()?;
                let map_duration = map_start.elapsed(); // Calculate duration
                println!("Time to map tree: {:?}", map_duration);
                total_map_duration += map_duration;
                data.push(vals);
                println!("Tree size: {:?}", tree_db.size());
                println!("--------------------");
            }
        }

        println!("Avg open time: {:?}", total_open_duration.as_millis() as f32 / data.len() as f32);
        println!("Avg map time: {:?}", total_map_duration.as_millis() as f32 / data.len() as f32);
        */

        let load_duration = load_start.elapsed(); // Calculate duration
        let print_start = Instant::now(); // Start timing
        CommitMerkleTree::print_depth(&root, depth);
        let print_duration = print_start.elapsed(); // Calculate duration
        println!("Time to load tree: {:?}", load_duration);
        println!("Time to print tree: {:?}", print_duration);

        Ok(())
    }
}
