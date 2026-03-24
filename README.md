# Feature Transaction Pipeline (MBD-mini)

This project is a **Dagster + Polars pipeline** to compute daily and backfill features for client transaction data (MBD-mini dataset).  
It supports **daily execution** and **backfill mode**, is **containerized with Docker**, and optionally deployable on **Kubernetes**.

---
## 1. Project Structure

```text
feature-transaction-pipeline/
│
├── src/
│   └── feature_transaction_pipeline/
│       ├── __init__.py
│       ├── definitions.py              # Dagster entry point
│       │
│       └── defs/                      # Assets & resources
│           ├── __init__.py
│           ├── assets.py
│           ├── resources.py
│           ├── schedules.py
│           ├── config.py
│           └── utils.py               # Core Polars logic
│
├── tests/
│   └── test_features.py               # Pytest suite
│
├── data/                              # Persistent data storage
│   └── detail/
│       ├── trx/
│         ├── fold=*/
│        
│       
│
├── Dockerfile                         # Multi-stage UV build
├── pyproject.toml
├── values.yaml                        # Helm config for Kubernetes
├── uv.lock
└── README.md

```
---

## 2. Dataset

Dataset comes from HuggingFace:

- [MBD-mini dataset](https://huggingface.co/datasets/ai-lab/MBD-mini/tree/main/detail.tar.gz)

- The `trx/` folder contains parquet files for the prototype pipeline.

Download and extract the dataset using Hugging Face Hub.

Run the `hf_hub_download_gz` function located in `utils.py`

---

## 3. Features

### Daily Mode
- Computes **daily aggregate features per client**:
  - `mean_amount`
  - `mean_amount_by_event`  

### Backfill Mode
- Computes **rolling monthly features** (30-day window):
  - `mean_amount_daily`
  - `mean_amount_30d`  

---

## 4. Build Docker Image

1. Navigate to project root:

```
cd feature-transaction-pipeline
docker build -t feature-pipeline:1 .
docker run -p 3030:3030 feature-pipeline:1
```

## 5. Local Run

### Installing dependencies


**Option 1: uv**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS          | Command |
|-------------| --- |
| MacOS/Linux | ```source .venv/bin/activate``` |
| Windows     | ```.venv\Scripts\activate``` |

**Option 2: pip**

Install the python dependencies with [pip](https://pypi.org/project/pip/):

```bash
python3 -m venv .venv
```

Then activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS/Linux| ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the required dependencies:

```bash
pip install -e ".[dev]"
```

### Running Dagster UI

Start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser to see the project.
#### In the UI, you can:
* Explore assets and pipelines
* Materialize assets
* Monitor runs and logs

## 6. Testing
### Unit Tests
The features tested using pytest with mocked DataFrames.
```
pyhtest tests/test_features.py
```

## 7. Kubernetes Deployment (Helm)
Install required tools following this instractions [deploying-to-kubernetes](https://docs.dagster.io/deployment/oss/deployment-options/kubernetes/deploying-to-kubernetes)

### Minikube Image Load:


build docker image 
```bash
eval $(minikube docker-env)
docker build -t feature-pipeline:1 .
```
### Deploy via Helm:

```bash
helm upgrade --install dagster dagster/dagster -f values.yaml
```
### Access UI:

```
export POD_NAME=$(kubectl get pods -l "component=dagster-webserver" -o jsonpath="{.items[0].metadata.name}")
kubectl port-forward $POD_NAME 8080:80

```

