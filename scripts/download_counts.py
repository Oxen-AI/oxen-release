import requests

repo_owner = "Oxen-AI"
repo_name = "Oxen"
url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/releases"
response = requests.get(url)
data = response.json()

total_downloads = 0
downloads_per_tag = {}

for release in data:
    tag_name = release["tag_name"]
    download_count = 0
    for asset in release["assets"]:
        download_count += asset["download_count"]
        total_downloads += asset["download_count"]
    downloads_per_tag[tag_name] = download_count
    

for tag_name, download_count in downloads_per_tag.items():
    print(f"{tag_name}: {download_count}")
print(f"Total Downloads: {total_downloads}")

