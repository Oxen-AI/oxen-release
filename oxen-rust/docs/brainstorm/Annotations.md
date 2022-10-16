
# Oxen Annotations

## Why Oxen

Companies have large sets of annotations, that change over time, given different models or workflows. Oxen helps you track changes to these annotations, giving attribution to who changed what, and letting you roll back to any version.

Annotations could come from models, and it would be nice to quickly find error cases and flag them for fixing. Human could fix...then eventually AI could fix?

Data Value Chain

- Who annotated?
- When annotated?

Operations we want to be able to do

- Iterate over annotations within schema (bounding box)
- Iterate over annotations within commit within schema
- Compare annotations between commits
    - Find me all the rows that reference file=="path/to/file.jpg" on this commit
    - Find me all the rows that reference label=="person" on this commit
- Find all annotations from a user (this is within a commit within a schema, already have)
- Find me all annotations that have a property
    - If we keep references to the csvs that they came from, we could join them all, and run a big query

- Find me all annotations from Greg
- Find me all annotations with field X
    - Would have to index on field X and know schema

How do we not save every complete version of the giant table if just one row changes?
......

FIRST STEP

1) If a tabular file is added/committed, we save it off as .arrow file in the versions directory with a _row_num projected on
    - This will not be the most efficient for *storage* on disk, but will allow for fast access over the data
    - We can quickly get to rows using polars.slice(offset,len) which is sweet
2) Add ability to quickly fetch indices from tabular file or paginate
    - /namespace/reponame/rows/commit/file/

-- RELEASE 

1) Let's do `oxen annotate -a <path/to/annotations.csv> -n "bounding_box" -f "file"` to keep track of individual annotations
    - Annotation object is just a link between a row in an .arrow table that is checked in, and the commit id, and a entry on disk
    - You should be able to add one from CLI too without reading from a file
        - OR just add a row to a file, add, commit, boom
    - You should be able to POST one from the API
        - Same thing, where do you want to store the annotation?
    - You should be able to list all annotations from a repo, hence the annotations/ dir in the commit history



Start with
    1) Look at a column with name, default to "file" field
        
        `oxen index -t path/to/annotations.csv -n "bounding_box" -f "file"`

    2) Save off the schema we just indexed for reference
        
        `oxen schema list`
        
        `oxen schema update eh219ehdj -n "bounding_box"`

    3) Save off row indicies and file content hash pointers to the annotations dir

        Ex) Query
            Find all the rows (annotations) that belong to a key-val pair

            Find all the rows that have the schema.bounding_box.file key == val

            Find all the rows that have the schema.bounding_box.label key == val

            Compare these between commits

            1) Does this schema name + key exist? find the schema hash
            2) Use the schema hash + index key to look up all the values associated with the val
            3) Find all row hashes for that commit (don't need to look in the actual file until you diff)
            4) Once you find the differing row hashes, you can find the actual rows


        .oxen/versions/FILE_HASH/
            contents.jpg

        .oxen/history/
            commit_id/ (has 3 top level objects "files", "dirs", "indexes")
                indexes/
                    schemas/
                        ROCKSDB
                        "schema_hash" => {
                                "name": "bounding_box",
                                "hash": "name,type,....->hashed",
                                "fields": [
                                    {"name": "file", "dtype":"string"},
                                    {"name": "x", "dtype":"f32"},
                                    {"name": "y", "dtype":"f32"},
                                    {"name": "w", "dtype":"f32"},
                                    {"name": "h", "dtype":"f32"},
                                ],
                            }

                    schema_indexes/
                        schema_hash/ (bounding_box)
                            ROCKSDB
                            "index_key" -> { timestamps ... }
                    schema_hash/
                        index_key/ ("file" or "label" or "whatever aggregate query you want")
                            index_val_hash/ ("path/to/file.jpg" or "person")
                                ROCKSDB
                                    row_hash => { (now we can diff between commits, based on the query)
                                        _row_num, (in .arrow file)
                                        arrow_file_content_hash, (to get to .arrow file path)
                                    }
                dirs/
                files/
                    path/
                        to/
                            dir/
                                file_name -> file_hash (get us to the versions dir)  



Would we want to index the filenames 

/path/to/file.jpg -> HASH
HASH -> /path/to/file.jpg

Find all the files that have a certain field in 

oxen query schema_name.category == Person
    - Slow
    - Could index to make faster

oxen index bbox.category

    person -> HASH_1,HASH_2,HASH_3
    car -> HASH_4
    elephant -> HASH_5

oxen search bbox.category == 'person'

# Annotation Obj

- _id
- created_at
- created_by
- schema_name
- file.csv
- hash
- body


# 

# Commands

`oxen add -a my_annotations.csv`

    - Flag for adding row level information on commit about who added the annotations
    - Make sure this is fast enough to add the columns and strip them for a diff

`oxen view my_annotations.csv`

    - pretty plot table
    - flags for viewing specific columns
        `oxen view -c my_annotations.csv` to see all columns

`oxen view file.jpg my_annotations.csv`
    - 

`oxen diff path/to/my_custom.csv COMMIT_ID|BRANCH_ID` # will show a tabular diff of annotation files

oxen status -a path/to/
    DIR        FILES             ANNOTATIONS

    raw/      +0   Δ0  -0        +0   -0
    videos/   +34  Δ0  -0        +20  -0
    images/   +100 Δ20 -10       +200 -10


    STATUS      FILE           ANNOTATIONS

    modified    image_1.jpg    +4 -0
    added       image_2.jpg    +1 -0
    added       image_3.jpg    +1 -0
    annotated   image_4.jpg    +1 -0

    ...

    annotated   image_4.jpg    +1 -0

Total 200 (some way of summarizing without seeing all, unless they ask)

# Example Directory Structures

QUESTION: Is this the most efficient way to do it?
    - performance on "add" command and "commit" command
        - ✅ Read csv into memory
        - ✅ Compare/diff columns in Apache Arrow
        - Read json into memory
        - Select on filename field
        - Add columns to csv to store in .parq in commit dir
        


## Bounding Box

PersonBoundingBox
    .oxen/
        staged/
            
        history/
            COMMIT_ID/
                commit_obj with stats
                .my_custom.parq
                
    raw/
        other_data.csv
    processed/
        my_custom.csv
        images/
            ai_challenger/
                train/
                    0000e06c1fc586992dc2445e9e102899ccb5e3fc.jpg
                    00039905a36b4d948d88d400ac367f0317057da2.jpg
                    00097c661b197ae6544c2c9322859e8e4a52f79e.jpg
                    0013c06cc742cd1717e418a080570437b416dd25.jpg
                    001628c80514838fe28bb7fedc669670bb96aab5.jpg
                valid/
                    001de8be03111be19762609b5ffd82e7011e6e8c.jpg
                    002184730e460196e25a08af6dc56e998fe369aa.jpg
                    0024a3c17e3122724dd9d6bc9d48108848ac644e.jpg
                    002748ef2f95c78d3f908b93034ade9d69d1e04c.jpg
                    00295b0d4748babe8717d610f870df3c7a6012de.jpg
            coco/
                train2017/
                    000000000001.jpg
                    000000000025.jpg
                    000000000030.jpg
                train2014/
                    COCO_train2014_000000000009.jpg
                    COCO_train2014_000000000025.jpg
                    COCO_train2014_000000000030.jpg
                val2017/
                    000000000139.jpg
                    000000000285.jpg
                    000000000632.jpg
                test2017/
                    000000000001.jpg
                    000000000016.jpg
                    000000000019.jpg
            leeds_sports/
                images/
                    im0001.jpg
                    im0002.jpg
                    im0003.jpg

## Pose Estimation On Video

PoseEstimation
    videos/
        practice_09_20_2022/
            .annotations.ndjson # could have high level meta data about the videos, but not frame level info
            video_1.mp4
            video_2.mp4
        practice_09_22_2022/
            .annotations.ndjson # could have high level meta data about the videos, but not frame level info
            video_1.mp4
            video_2.mp4
            video_3.mp4
    processed/
        practice_09_20_2022/
            video_1/
                .annotations.ndjson # frame level info for this practice video
                frame_0.png
                frame_1.png
                frame_2.png
            video_2/
                .annotations.ndjson # frame level info for this practice video
                frame_0.png
                frame_1.png
                frame_2.png


## Use Cases

## 1) Model Iteration

1) User has a directory of un-annotated videos
2) They run a baseline model on the directory to get a set of annotations
3) They store the outputs in a .annotations file that sits in the directory, next to the videos
4) They can use Oxen to get high level stats about the .annotations file
5) They commit the directory, attributing the initial annotations to the user that created them
6) When you "commit" it adds row level meta data of "committed_by", "committed_at", etc
7) When you commit it compresses and saves this version of the annotations to our .versions directory
8) They run a new model, you can overwrite the annotations without worry, you can always revert
9) You can diff this directory of annotations, it is smart enough to ignore the committed_by and committed_at columns but diff the rest.

## 2) Sharing Tabular Data, without versioning 09-15-2022.tsv 09-16-2022.tsv

1) User has sets of tabular data they want to share
2) oxen add rti_rarity.csv
3) oxen commit -m "adding rarity data exported on 09-15-2022"
4) oxen push origin main # distribute out to participants
5) Overwrite the rarity data file, however you see fit, we have the backup so it's fine
6) oxen diff rti_rarity.csv # this knows it is a csv, so can do some fun manipulation with apache arrow and show differences
7) oxen add rti_rarity.csv
8) oxen commit -m "adding rarity data exported on 09-16-2022"
9) oxen push origin main # distribute out to particiopants

## 3) Dataset Curation

1) I have a bunch of raw data that is not linked to oxen .annotation files
2) I check in the raw data as is, so that I can always go back to it
3) Do we have some sort of command to "freeze" this data, so that we never check it again? That would help with efficiency
4) I write some code to process the data, and generate .annotation files
5) We know that the .annotation files reference paths down from where they sit
6) You could have one massive .annotation file at the top level if you really wanted to, but when you use the `oxen annotate path/to/file.jpg '{ ... }'` command, we just append it to the file there.
    - Random Thought: We don't have to really worry about concurrency on that file, because you are just working locally, and have to commit to push or collab...
7) I move the .annotation files to the proper directory, and `oxen add` them. Oxen `add` and `commit` recognize these as special files and will watch them during a `status` command.
8) `oxen status` will take a directory as input for efficiency



- Advantages
    - We limit the resources to that dir
    - Gives flexibility in how much we load into memory
    - We could more easily diff per directory
    - We can do some map reduce operations to get global counts to sum up dirs

- Disadvantages

- What would a change look like?
    - Adding annotations...
        - Append to the CSV file in that dir


# What do we want to be able to do

- List annotations per file
    - Given file path, find all annotations
    - Do we have one big annotations file per commit? Per directory?
        - How do we store this to efficiently load?
        - Do we upcharge for loading a lot into memory?
        - Let the user configure their environment to view?
        - What does the Jupyter Notebook integration look like?
            - "I don't want to have to think about if my data is local or remote" - Kiel

- Compare annotations between runs
    - Compare ground truth to model predictions
    - Compare model 1 predictions to model 2 predictions
    - Calculate P/R/A
        - Calculate TP,FN,FP,TN
    - Psuedo Code
        for (row_1, row_2) in zip(dataset_1, dataset_2):
            if row_1.compare(row_2) == TRUE_PA