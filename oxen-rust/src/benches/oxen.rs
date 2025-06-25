use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;
use liboxen::util;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::path::{Path, PathBuf};

fn generate_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn setup_repo_for_add_benchmark(
    base_dir: &Path,
    repo_size: usize,
    num_files_to_add_in_benchmark: usize,
) -> Result<(LocalRepository, Vec<PathBuf>, PathBuf), OxenError> {
    let repo_dir = base_dir.join(format!("repo_{}", num_files_to_add_in_benchmark));
    if repo_dir.exists() {
        util::fs::remove_dir_all(&repo_dir)?;
    }

    let repo = repositories::init(&repo_dir)?;

    let files_dir = repo_dir.join("files");
    util::fs::create_dir_all(&files_dir)?;

    let mut rng = rand::thread_rng();
    // Create a number of directories up to 4 levels deep
    let mut dirs: Vec<PathBuf> = (0..20)
        .map(|_| {
            let mut path = files_dir.clone();
            let depth = rng.gen_range(1..=4);
            for _ in 0..depth {
                path = path.join(generate_random_string(10));
            }
            path
        })
        .collect();
    dirs.push(files_dir.clone());

    for i in 0..repo_size {
        let dir_idx = rng.gen_range(0..dirs.len());
        let dir = &dirs[dir_idx];
        util::fs::create_dir_all(dir)?;
        let file_path = dir.join(format!("file_{}.txt", i));
        util::fs::write_to_path(&file_path, "this is a test file")?;
    }
    repositories::add(&repo, black_box(&files_dir))?;
    repositories::commit(&repo, "Init")?;

    for i in repo_size..(repo_size + num_files_to_add_in_benchmark) {
        let dir_idx = rng.gen_range(0..dirs.len());
        let dir = &dirs[dir_idx];
        util::fs::create_dir_all(dir)?;
        let file_path = dir.join(format!("file_{}.txt", i));
        util::fs::write_to_path(&file_path, "this is a new test file to be added")?;
    }
    Ok((repo, dirs, files_dir))
}

fn add_benchmark(c: &mut Criterion) {
    let base_dir = PathBuf::from("data/test/benches/add");
    if base_dir.exists() {
        util::fs::remove_dir_all(&base_dir).unwrap();
    }
    util::fs::create_dir_all(&base_dir).unwrap();

    let mut group = c.benchmark_group("add");
    for repo_size in [1000, 10000, 100000].iter() {
        let num_files_to_add = repo_size / 100;
        let (repo, _, file_dir) =
            setup_repo_for_add_benchmark(&base_dir, *repo_size, num_files_to_add).unwrap();

        group.bench_with_input(
            BenchmarkId::from_parameter(num_files_to_add),
            repo_size,
            |b, _| {
                b.iter(|| {
                    repositories::add(&repo, black_box(&file_dir)).unwrap();

                    let _ = util::fs::remove_dir_all(repo.path.join(".oxen/staging"));
                })
            },
        );
    }
    group.finish();

    // Cleanup
    util::fs::remove_dir_all(base_dir).unwrap();
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
