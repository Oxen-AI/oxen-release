use liboxen::command;
use liboxen::error::OxenError;
use liboxen::test;

use std::path::Path;

#[test]
fn test_command_schema_list() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let schemas = command::schemas::list(&repo, None)?;
        assert_eq!(schemas.len(), 3);

        let schema = command::schemas::get_from_head(&repo, "bounding_box")?.unwrap();

        assert_eq!(schema.hash, "b821946753334c083124fd563377d795");
        assert_eq!(schema.fields.len(), 6);
        assert_eq!(schema.fields[0].name, "file");
        assert_eq!(schema.fields[0].dtype, "str");
        assert_eq!(schema.fields[1].name, "label");
        assert_eq!(schema.fields[1].dtype, "str");
        assert_eq!(schema.fields[2].name, "min_x");
        assert_eq!(schema.fields[2].dtype, "f64");
        assert_eq!(schema.fields[3].name, "min_y");
        assert_eq!(schema.fields[3].dtype, "f64");
        assert_eq!(schema.fields[4].name, "width");
        assert_eq!(schema.fields[4].dtype, "i64");
        assert_eq!(schema.fields[5].name, "height");
        assert_eq!(schema.fields[5].dtype, "i64");

        Ok(())
    })
}

#[test]
fn test_stage_and_commit_schema() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        // Make sure no schemas are staged
        let status = command::status(&repo)?;
        assert_eq!(status.added_schemas.len(), 0);

        // Make sure no schemas are committed
        let schemas = command::schemas::list(&repo, None)?;
        assert_eq!(schemas.len(), 0);

        // Schema should be staged when added
        let bbox_filename = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_file = repo.path.join(bbox_filename);
        command::add(&repo, bbox_file)?;

        // Make sure it is staged
        let status = command::status(&repo)?;
        assert_eq!(status.added_schemas.len(), 1);
        for (path, schema) in status.added_schemas.iter() {
            println!("GOT SCHEMA {path:?} -> {schema:?}");
        }

        // name the schema when staged
        let schema_ref = "b821946753334c083124fd563377d795";
        let schema_name = "bounding_box";
        command::schemas::set_name(&repo, schema_ref, schema_name)?;

        // Schema should be committed after commit
        command::commit(&repo, "Adding bounding box schema")?;

        // Make sure no schemas are staged after commit
        let status = command::status(&repo)?;
        assert_eq!(status.added_schemas.len(), 0);

        // Fetch schema from HEAD commit
        let schema = command::schemas::get_from_head(&repo, "bounding_box")?.unwrap();

        assert_eq!(schema.hash, "b821946753334c083124fd563377d795");
        assert_eq!(schema.fields.len(), 6);
        assert_eq!(schema.fields[0].name, "file");
        assert_eq!(schema.fields[0].dtype, "str");
        assert_eq!(schema.fields[1].name, "label");
        assert_eq!(schema.fields[1].dtype, "str");
        assert_eq!(schema.fields[2].name, "min_x");
        assert_eq!(schema.fields[2].dtype, "f64");
        assert_eq!(schema.fields[3].name, "min_y");
        assert_eq!(schema.fields[3].dtype, "f64");
        assert_eq!(schema.fields[4].name, "width");
        assert_eq!(schema.fields[4].dtype, "i64");
        assert_eq!(schema.fields[5].name, "height");
        assert_eq!(schema.fields[5].dtype, "i64");

        Ok(())
    })
}
