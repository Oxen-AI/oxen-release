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
        add_row(repo, input.as_ref(), row).await
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

// TODO: Only difference between this and `df` is for `get` operations - everything above
// the "else" can be factored into a shared method
pub async fn staged_df<P: AsRef<Path>>(
    repo: &LocalRepository,
    input: P,
    opts: DFOpts,
) -> Result<DataFrame, OxenError> {
    // Special case where we are writing data
    let identifier = UserConfig::identifier()?;
    if let Some(row) = &opts.add_row {
        add_row(repo, input.as_ref(), row).await
    } else if let Some(uuid) = &opts.delete_row {
        delete_row(repo, input, uuid).await
    } else {
        let remote_repo = api::remote::repositories::get_default_remote(repo).await?;
        let branch = api::local::branches::current_branch(repo)?.unwrap();
        let output = opts.output.clone();
        let val =
            api::remote::df::get_staged(&remote_repo, &branch.name, &identifier, input, opts).await;
        if let Ok(val) = val {
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
        } else {
            println!(
                    "Dataset not indexed for remote editing. Use `oxen df --index <path>` to index it, or `oxen df <path> --committed` to view the committed resource in view-only mode.\n"
                );
            Err(OxenError::basic_str("No dataset staged for this resource."))
        }
    }
}

pub async fn add_row(repo: &LocalRepository, path: &Path, data: &str) -> Result<DataFrame, OxenError> {
    let remote_repo = api::remote::repositories::get_default_remote(repo).await?;

    // let data = format!(r#"{{"data": {}}}"#, data);
    let data = data.to_string();

    if let Some(branch) = api::local::branches::current_branch(repo)? {
        let user_id = UserConfig::identifier()?;
        let (df, row_id) = api::remote::staging::modify_df(
            &remote_repo,
            &branch.name,
            &user_id,
            path,
            data,
            crate::model::ContentType::Json,
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

pub async fn get_row(
    repository: &LocalRepository,
    path: impl AsRef<Path>,
    row_id: &str,
) -> Result<DataFrame, OxenError> {
    let remote_repo = api::remote::repositories::get_default_remote(repository).await?;
    if let Some(branch) = api::local::branches::current_branch(repository)? {
        let user_id = UserConfig::identifier()?;
        let df_json = api::remote::staging::get_row(
            &remote_repo,
            &branch.name,
            &user_id,
            path.as_ref(),
            row_id,
        )
        .await?;
        let df = df_json.data_frame.view.to_df();
        println!("{:?}", df);
        Ok(df)
    } else {
        Err(OxenError::basic_str(
            "Must be on a branch to stage remote changes.",
        ))
    }
}

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
