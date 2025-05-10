# dlt-advanced-2025

This project contains a dlt pipeline that extracts data from the [Jaffle Shop API](https://jaffle-shop.scalevector.ai/docs).

## Overview

The pipeline extracts data from the following endpoints:

*   `/customers`
*   `/orders`
*   `/products`

The data is loaded into a DuckDB database.

## Project Structure

*   [`main.py`](main.py): Contains the dlt pipeline definition.
*   [`pipeline.ipynb`](pipeline.ipynb): Contains a Jupyter Notebook with the dlt pipeline implementation and performance optimization techniques.
*   [`pyproject.toml`](pyproject.toml): Contains project metadata and dependencies.
*   [`requirements_github_action.txt`](requirements_github_action.txt): Contains the project dependencies for GitHub Actions.
*   [`uv.lock`](uv.lock): Contains the locked dependencies for the project.

## Dependencies

The project uses the following dependencies:

*   `dlt[cli,duckdb]>=1.10.0`
*   `ipykernel>=6.29.5`

## Performance Optimization

The `pipeline.ipynb` notebook demonstrates performance optimization techniques for the dlt pipeline, including:

*   Chunking
*   Parallelism
*   Buffer control
*   File rotation
*   Worker tuning

The optimized pipeline reduces the overall pipeline runtime from 16 minutes and 52.11 seconds down to 7 minutes and 35.18 seconds.

## Usage

To run the pipeline, execute the `main.py` script:

```bash
python main.py
