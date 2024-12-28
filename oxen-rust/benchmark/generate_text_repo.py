import os
import argparse
from tqdm import tqdm
import pandas as pd

def generate_text_files(num_files, output_dir):
    print(f"Generating {num_files} text files in {output_dir}")
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Make the texts dir
    texts_dir = os.path.join(output_dir, "texts")
    os.makedirs(texts_dir, exist_ok=True)

    # return all the text file paths
    file_paths = []
    for i in tqdm(range(num_files)):
        # Generate minimal content (just a number)
        content = f"File content {i}"

        # Save the text file
        path = os.path.join(texts_dir, f"file_{i}.txt")
        with open(path, 'w') as f:
            f.write(content)

        # Get the relative path to the output_dir
        relative_path = os.path.relpath(path, output_dir)
        file_paths.append(relative_path)

    return file_paths

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_files", type=int, default=10000)
    parser.add_argument("--output_dir", type=str, default="text_files")
    args = parser.parse_args()

    file_paths = generate_text_files(args.num_files, args.output_dir)
    print("Text file generation complete!")

    # create random labels for each file of positive or negative
    labels = np.random.choice(["positive", "negative"], size=args.num_files)

    # write dataframe
    df = pd.DataFrame({"files": file_paths, "labels": labels})
    df.to_csv(os.path.join(args.output_dir, "files.csv"), index=False)

    with open(os.path.join(args.output_dir, "README.md"), "w") as f:
        f.write(f"# Sample Repo\n\nGenerated {args.num_files} text files in {args.output_dir}")