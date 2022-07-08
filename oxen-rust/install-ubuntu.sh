
# SSH Keys
ssh-keygen -t ed25519 -C "g@oxen.ai"
ssh-add -K ~/.ssh/id_ed25519
# Copy into deploy keys on github
cat ~/.ssh/id_ed25519.pub

# Clone the repo
mkdir Code
cd Code/
git clone git@github.com:Oxen-AI/Oxen.git
cd Oxen/

# Install the deps
sudo apt-get install --assume-yes apt-utils
sudo apt-get install -y libclang-dev
sudo apt install build-essential
sudo apt install libssl-dev
sudo apt install pkg-config
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
cargo install cargo-build-deps

# Build the server
cargo build --release

# Run the server with a user
mkdir /home/ubuntu/Data/sync/
sudo env SYNC_DIR=/home/ubuntu/Data/sync/ ./target/release/oxen-server add-user --email ox@oxen.ai --name Ox --output auth_config.toml
sudo env SYNC_DIR=/home/ubuntu/Data/sync/ ./target/release/oxen-server start -p 80

