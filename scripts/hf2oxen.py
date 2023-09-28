import datasets
import argparse

# argparse the name of the dataset
parser = argparse.ArgumentParser(description='Download a dataset from hugging face and upload to Oxen.')
# parse dataset as -d or --dataset
parser.add_argument('-d','--dataset', dest="dataset", required=True, help="Name of the dataset to download from hugging face")
args = parser.parse_args()

# download the dataset from hugging face
hf_dataset = datasets.load_dataset(args.dataset)

# export the dataset to a parquet file
hf_dataset.export_to_file(f"{args.dataset}.parquet")