---

# PySpark Aidetic Assignment

This repository contains a PySpark assignment for Aidetic Inc.

## Setup Instructions

Follow the steps below to set up and run the PySpark job locally.

## Prerequisites

- Apache Spark installed on your local machine.
- Git installed to clone the repository.
- Python and Poetry for managing virtual environments and packages.

### 1. Clone the Repository

Clone this repository to your local machine using the following command:

```bash
git clone <repository_url>
```

### 2. Create a Virtual Environment

Create a Python virtual environment to isolate the project dependencies. You can use `venv` or `virtualenv` for this purpose.

```bash
python3 -m venv .venv
```

### 3. Install Packages Using Poetry

Navigate to the project directory and activate the virtual environment. Then, use Poetry to install the required packages.

```bash
source .venv/bin/activate
poetry install
```

### 4. Create `packages.zip` from virtual environment

Once the packages are installed, create a `packages.zip` file containing the dependencies from the virtual environment.
Make sure to include the `dependency.py` in `packages.zip`.

### 5. Run the Job

Finally, run the PySpark job from the project's root directory using the following command:

```bash
spark-submit --master local[*] --py-files src/pyspark_aidetic_assignment/packages.zip src/pyspark_aidetic_assignment/job.py
```

Replace `<repository_url>` with the actual URL of the repository.

## Contact

For any issues or inquiries, please contact [irfankalam237@gmail.com](mailto:author_email).

---

Feel free to customize this README file with additional information or formatting as needed for your project.