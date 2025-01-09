use crate::model::DataTypeStat;
use crate::model::EntryDataType;
use crate::model::LocalRepository;
use crate::model::RepoStats;
use crate::repositories;
use crate::util;
use std::collections::HashMap;

pub fn get_stats(repo: &LocalRepository) -> RepoStats {
    let mut data_size: u64 = 0;
    let mut data_types: HashMap<EntryDataType, DataTypeStat> = HashMap::new();

    match repositories::commits::head_commit(repo) {
        Ok(commit) => match repositories::entries::list_for_commit(repo, &commit) {
            Ok(entries) => {
                for entry in entries {
                    data_size += entry.num_bytes;
                    let full_path = repo.path.join(&entry.path);
                    let data_type = util::fs::file_data_type(&full_path);
                    let data_type_stat = DataTypeStat {
                        data_size: entry.num_bytes,
                        data_type: data_type.to_owned(),
                        file_count: 1,
                    };
                    let stat = data_types.entry(data_type).or_insert(data_type_stat);
                    stat.file_count += 1;
                    stat.data_size += entry.num_bytes;
                }
            }
            Err(err) => {
                log::error!("Err: could not list entries for repo stats {err}");
            }
        },
        Err(err) => {
            log::error!("Err: could not get repo stats {err}");
        }
    }

    RepoStats {
        data_size,
        data_types,
    }
}
