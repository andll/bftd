#!/bin/bash -oe

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
. "$HOME/.cargo/env"
sudo apt-get update
sudo apt-get install build-essential prometheus iftop clang -y
sudo mv ~/data/prometheus.yml /etc/prometheus/prometheus.yml
sudo service prometheus reload
cd ~/bftd && cargo build --release
