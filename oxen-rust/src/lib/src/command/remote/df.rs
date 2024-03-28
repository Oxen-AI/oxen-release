//! # oxen df
//!
//! Interact with Remote DataFrames
//!

use std::path::Path;

use polars::prelude::DataFrame;

use crate::api;
use crate::config::UserConfig;
use crate::core::df::tabular;
use crate::error::OxenError;
use crate::model::entry::mod_entry::ModType;
use crate::model::LocalRepository;
use crate::opts::DFOpts;

/// Interact with Remote DataFrames
pub async fn df<P: AsRef<Path>>(
    repo: &LocalRepository,
    input: P,
    opts: DFOpts,
) -> Result<DataFrame, OxenError> {
    // Special case where we are writing data
    if let Some(row) = &opts.add_row {
        add_row(repo, input.as_ref(), row, &opts).await
    } else if let Some(uuid) = &opts.delete_row {
        delete_row(repo, input, uuid).await
    } else {
        let remote_repo = api::remote::repositories::get_default_remote(repo).await?;
        let branch = api::local::branches::current_branch(repo)?.unwrap();
        let output = opts.output.clone();
        let val = api::remote::df::get(&remote_repo, &branch.name, input, opts).await?;
        let mut df = val.data_frame.view.to_df();
        if let Some(output) = output {
            println!("Writing {output:?}");
            tabular::write_df(&mut df, output)?;
        }

        println!(
            "Full shape: ({}, {})\n",
            val.data_frame.source.size.height, val.data_frame.source.size.width
        );
        println!("Slice {df:?}");
        Ok(df)
    }
}

pub async fn staged_df<P: AsRef<Path>>(
    repo: &LocalRepository,
    input: P,
    opts: DFOpts,
) -> Result<DataFrame, OxenError> {
    // Special case where we are writing data
    log::debug!("df got opts {:?}", opts);
    let identifier = UserConfig::identifier()?;
    if let Some(row) = &opts.add_row {
        add_row(repo, input.as_ref(), row, &opts).await
    } else if let Some(uuid) = &opts.delete_row {
        delete_row(repo, input, uuid).await
    } else {
        let remote_repo = api::remote::repositories::get_default_remote(repo).await?;
        let branch = api::local::branches::current_branch(repo)?.unwrap();
        let output = opts.output.clone();
        let val = api::remote::df::get_staged(&remote_repo, &branch.name, &identifier, input, opts)
            .await?;
        let mut df = val.data_frame.view.to_df();
        if let Some(output) = output {
            println!("Writing {output:?}");
            tabular::write_df(&mut df, output)?;
        }

        println!(
            "Full shape: ({}, {})\n",
            val.data_frame.source.size.height, val.data_frame.source.size.width
        );
        println!("Slice {df:?}");
        Ok(df)
    }
}

async fn add_row(
    repo: &LocalRepository,
    path: &Path,
    data: &str,
    opts: &DFOpts,
) -> Result<DataFrame, OxenError> {
    let remote_repo = api::remote::repositories::get_default_remote(repo).await?;

    let data = format!(r#"{{"data": {}}}"#, data);

    if let Some(branch) = api::local::branches::current_branch(repo)? {
        let user_id = UserConfig::identifier()?;
        let (df, row_id) = api::remote::staging::modify_df(
            &remote_repo,
            &branch.name,
            &user_id,
            path,
            data,
            opts.content_type.to_owned(),
            ModType::Append,
        )
        .await?;

        if let Some(row_id) = row_id {
            println!("\nAdded row: {row_id:?}");
        }

        println!("{:?}", df);
        Ok(df)
    } else {
        Err(OxenError::basic_str(
            "Must be on a branch to stage remote changes.",
        ))
    }
}
pub async fn delete_row(
    repository: &LocalRepository,
    path: impl AsRef<Path>,
    uuid: &str,
) -> Result<DataFrame, OxenError> {
    let remote_repo = api::remote::repositories::get_default_remote(repository).await?;
    if let Some(branch) = api::local::branches::current_branch(repository)? {
        let user_id = UserConfig::identifier()?;
        let df = api::remote::staging::rm_df_mod(&remote_repo, &branch.name, &user_id, path, uuid)
            .await?;
        Ok(df)
    } else {
        Err(OxenError::basic_str(
            "Must be on a branch to stage remote changes.",
        ))
    }
}

// pub async fn delete_staged_row(
//     repository: &LocalRepository,
//     path: impl AsRef<Path>,
//     uuid: &str,
// ) -> Result<DataFrame, OxenError> {
//     let remote_repo = api::remote::repositories::get_default_remote(repository).await?;
//     if let Some(branch) = api::local::branches::current_branch(repository)? {
//         let user_id = UserConfig::identifier()?;
//         let modification =
//             api::remote::staging::rm_df_mod(&remote_repo, &branch.name, &user_id, path, uuid)
//                 .await?;
//         modification.to_df()
//     } else {
//         Err(OxenError::basic_str(
//             "Must be on a branch to stage remote changes.",
//         ))
//     }
// }

pub async fn index_dataset(
    repository: &LocalRepository,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let remote_repo = api::remote::repositories::get_default_remote(repository).await?;
    if let Some(branch) = api::local::branches::current_branch(repository)? {
        let user_id = UserConfig::identifier()?;

        api::remote::staging::dataset::index_dataset(
            &remote_repo,
            &branch.name,
            &user_id,
            path.as_ref(),
        )
        .await
    } else {
        Err(OxenError::basic_str(
            "Must be on a branch to stage remote changes.",
        ))
    }
}
