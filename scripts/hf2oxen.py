
import argparse
import os
from huggingface_hub import list_repo_refs, HfApi
from datasets import load_dataset
from oxen.remote_repo import create_repo, get_repo
from oxen import LocalRepo

def human_size(bytes, units=[' bytes','KB','MB','GB','TB', 'PB', 'EB']):
    """ Returns a human readable string representation of bytes """
    return str(bytes) + units[0] if bytes < 1024 else human_size(bytes>>10, units[1:])

def get_dataset_info(dataset_name):
    import requests
    # headers = {"Authorization": f"Bearer {API_TOKEN}"}
    headers = {}
    API_URL = f"https://datasets-server.huggingface.co/info?dataset={dataset_name}"
    def query():
        response = requests.get(API_URL, headers=headers)
        return response.json()
    data = query()
    return data

# argparse the name of the dataset
parser = argparse.ArgumentParser(description='Download a dataset from hugging face and upload to Oxen.')
# parse dataset as -d or --dataset
parser.add_argument('-d','--dataset', dest="dataset", required=True, help="Name of the dataset to download from hugging face")
parser.add_argument('-o','--output', dest="output", required=True, help="The output directory to save the dataset to")
parser.add_argument('-n', '--namespace', dest="namespace", default="ox", help="The oxen namespace to upload to")
parser.add_argument('--host', dest="host", default="hub.oxen.ai", help="The host to upload to")
args = parser.parse_args()

dataset_name = args.dataset
output_dir = args.output
namespace = args.namespace
host = args.host

api = HfApi()

info = api.repo_info(dataset_name, repo_type="dataset")
print(info)
print(info.description)
commits = api.list_repo_commits(dataset_name, repo_type="dataset")
commits.reverse()
print(f"Got {len(commits)} commits")

info = get_dataset_info(dataset_name)
print(info)
sizes = []
for key in info['dataset_info'].keys():
    info_obj = info['dataset_info'][key]
    if 'size_in_bytes' in info_obj:
        size_in_bytes = info_obj['size_in_bytes']
    else:
        size_in_bytes = info_obj['dataset_size']
    print(f"{key}: {human_size(size_in_bytes)}")
    sizes.append(size_in_bytes)
sum_sizes = sum(sizes)
print(f"Dataset size: {human_size(sum_sizes)}")

if sum_sizes > 5_000_000_000:
    print(f"Dataset size is {human_size(sum_sizes)}, this is greater than 5GB, do not continue")
    exit(1)

# if dir exists, do not continue
output_dir = os.path.join(output_dir, dataset_name)
if os.path.exists(output_dir):
    print(f"Directory {output_dir} exists, do not continue")
    exit(1)

clean_name = dataset_name
if "/" in clean_name:
    clean_name = dataset_name.replace("/", "_")

name = f"{namespace}/{clean_name}"
# Create Remote Repo
if get_repo(name, host=host):
    print(f"Repo {name} exists, do not continue")
    exit(1)

# create dir
os.makedirs(output_dir)

# TODO: Create repo with description and README.md based off of contents of dataset info
remote_repo = create_repo(name, host=host)
local_repo = LocalRepo(output_dir)
local_repo.init()
local_repo.set_remote("origin", remote_repo.url())

for commit in commits:
    print(f"Loading commit: {commit}...")
    
    # download the dataset from hugging face
    try:
        hf_dataset = load_dataset(dataset_name, revision=commit.commit_id)
        print(hf_dataset)
        for key, dataset in hf_dataset.items():
            filename = os.path.join(output_dir, f"{key}.parquet")
            dataset.to_parquet(filename)
            local_repo.add(filename)

    except Exception as e:
        print(f"Got Exception: {e}")
        error_str = f"{e}"
        split_str = "Please pick one among the available configs: ["
        if split_str in error_str:
            config_options = error_str.split(split_str)[-1]
            config_options = config_options.split("]")[0]
            print(f"Available configs for {dataset_name}: {config_options}")
            options = config_options.split(",")
            for option in options:
                option = option.replace("'", "").strip()
                print(f"Download dataset {dataset_name} with option {option}")
                hf_dataset = load_dataset(dataset_name, option, revision=commit.commit_id)
                print(hf_dataset)
                
                # info = hf_dataset.info
                # print(info)
                
                for key, dataset in hf_dataset.items():
                    filename = os.path.join(output_dir, f"{key}_{option}.parquet")
                    dataset.to_parquet(filename)
                    local_repo.add(filename)
    except:
        print(f"Failed to download dataset {dataset_name} with commit {commit}")
        continue

    status = local_repo.status()
    commit_message = f"{commit.title}\n\n{commit.message}"
    if status.is_dirty():
        print(f"✅ Committing with message: {commit_message}...")
        
        if commit_message == "":
            commit.message = f"Update dataset {commit.commit_id}"
        
        local_repo.commit(commit_message)
    else:
        print(f"🤷‍♂️ Skipping commit with message: {commit_message}...")

print(f"Uploading {dataset_name} to {host}...")
local_repo.push()

