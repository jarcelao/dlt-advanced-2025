#!/bin/bash

set -e

sudo apt update

curl -LsSf https://astral.sh/uv/install.sh | sh

echo 'eval "$(uv generate-shell-completion bash)"' >> ~/.bashrc
eval "$(uv generate-shell-completion bash)"

uv sync
