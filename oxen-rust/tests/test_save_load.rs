// use std::path::Path;

// use liboxen::api;
// use liboxen::command;
// use liboxen::error::OxenError;
// use liboxen::model::LocalRepository;
// use liboxen::test;
// use liboxen::util;

// #[test]
// fn test_command_save_repo() -> Result<(), OxenError> {
//     test::run_empty_local_repo_test(|repo| {
//         // Write one file
//         let hello_file = repo.path.join("hello.txt");
//         util::fs::write_to_path(&hello_file, "Hello World")?;
//         // Add-commit
//         command::add(&repo, &hello_file)?;
//         command::commit(&repo, "Adding hello file")?;

//         // Save to a path
//         let save_path = Path::new("backup.tar.gz");
//         command::save(&repo, save_path)?;

//         assert!(save_path.exists());

//         Ok(())
//     })
// }

// #[test]
// fn test_command_save_load_repo_with_working_dir() -> Result<(), OxenError> {
//     test::run_empty_local_repo_test(|repo| {
//         test::run_empty_dir_test(|dir| {
//             // Write one file
//             let hello_file = repo.path.join("hello.txt");
//             util::fs::write_to_path(&hello_file, "Hello World")?;
//             // Add-commit
//             command::add(&repo, &hello_file)?;
//             command::commit(&repo, "Adding hello file")?;

//             // Save to a path
//             let save_path = dir.join(Path::new("backup.tar.gz"));
//             command::save(&repo, &save_path)?;

//             // Load from a path and hydrate
//             let loaded_repo_path = dir.join(Path::new("loaded_repo"));
//             command::load(&save_path, &loaded_repo_path, false)?;

//             let hydrated_repo = LocalRepository::from_dir(&loaded_repo_path)?;
//             assert!(hydrated_repo.path.join("hello.txt").exists());

//             Ok(())
//         })

//     })
// }

// #[test]
// fn test_command_save_load_repo_no_working_dir() -> Result<(), OxenError> {
//     test::run_empty_local_repo_test(|repo| {
//         test::run_empty_dir_test(|dir|  {
//             // Write one file
//             let hello_file = repo.path.join("hello.txt");
//             util::fs::write_to_path(&hello_file, "Hello World")?;
//             // Add-commit
//             command::add(&repo, &hello_file)?;
//             command::commit(&repo, "Adding hello file")?;

//             // Save to a path
//             let save_path = Path::new("backup.tar.gz");
//             command::save(&repo, save_path)?;

//             // Load from a path and hydrate
//             let loaded_repo_path = dir.join(Path::new("loaded_repo"));
//             command::load(save_path, &loaded_repo_path, true)?;

//             let hydrated_repo = LocalRepository::from_dir(&loaded_repo_path)?;

//             assert_eq!(hydrated_repo.path.join("hello.txt").exists(), false);

//             // Should have `hello.txt` in removed files bc it's in commits db but not working dir
//             let status = command::status(&hydrated_repo)?;

//             assert_eq!(status.removed_files.len(), 1);

//             Ok(())
//         })

//     })
// }

// #[test]
// fn test_command_save_load_moved_and_removed() -> Result<(), OxenError> {
//     test::run_empty_local_repo_test(|repo| {
//         test::run_empty_dir_test(|dir|  {
//             // Write one file
//             let hello_file = repo.path.join("hello.txt");
//             let goodbye_file = repo.path.join("goodbye.txt");
//             util::fs::write_to_path(&hello_file, "Hello World")?;
//             util::fs::write_to_path(&goodbye_file, "Goodbye World")?;
//             // Add-commit
//             command::add(&repo, &hello_file)?;
//             command::add(&repo, &goodbye_file)?;
//             command::commit(&repo, "Adding hello file")?;

//             // Move hello into a folder
//             let hello_dir = repo.path.join("hello_dir");
//             std::fs::create_dir(&hello_dir)?;
//             let moved_hello = hello_dir.join("hello.txt");
//             std::fs::rename(&hello_file, &moved_hello)?;

//             // Remove goodbye
//             std::fs::remove_file(&goodbye_file)?;

//             // Add a third file
//             let third_file = repo.path.join("third.txt");
//             util::fs::write_to_path(&third_file, "Third File")?;

//             // Add-commit
//             command::add(&repo, &moved_hello)?;
//             command::add(&repo, &hello_file)?;
//             command::add(&repo, &goodbye_file)?;
//             command::add(&repo, &third_file)?;
//             command::commit(&repo, "Moving hello file")?;

//             // Save to a path
//             let save_path = Path::new("backup.tar.gz");
//             command::save(&repo, save_path)?;

//             // Load from a path and hydrate
//             let loaded_repo_path = dir.join(Path::new("loaded_repo"));
//             command::load(save_path, &loaded_repo_path, false)?;

//             let hydrated_repo = LocalRepository::from_dir(&loaded_repo_path)?;

//             assert_eq!(hydrated_repo.path.join("third.txt").exists(), true);
//             assert_eq!(hydrated_repo.path.join("hello_dir/hello.txt").exists(), true);
//             assert_eq!(hydrated_repo.path.join("hello.txt").exists(), false);
//             assert_eq!(hydrated_repo.path.join("goodbye.txt").exists(), false);

//             Ok(())
//         })

//     })
// }
