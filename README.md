# Getting set up.
#### Assumptions
* You are using either Windows or MacOSX environment
* You have the following dependencies installed, on your local machine:
    * Windows:
        * Git, and Git Shell
        * Python 2
        * Docker (Windows installation of docker is OOS for this README)
        * Docker Quickstart Terminal
        * Kitematic
    * MacOSX:
        * Git
        * Python 2
        * Docker and dependencies
* You have a `~/Code` folder on your machine.
    * MAC: This would be at `/Users/{Your User Name}/Code
    * Windows: `C:\Users\{Your User Name}`

#### Setting up the repos:
* `cd ~/Code`
* `mkdir -p sd_airflow`
* `cd sd_airflow`
* `Seed your prod data`


# Dag Committing Checklist
* Have I created a folder for my DAG?
* Does my folder have an `__init__.py` file?
* Does my folder have `_dags.py` file where I define the dags?
* In my `_dags.py` file, have I:
    * Defined a DAG?
    * Is my LocalOnly operator the first task of the dag (if dag requires it)?
    * Created a task for ***extracting*** the data out of a place?
    * Created a task for ***transforming*** the data and storing it temporarily?
    * Created a task for ***loading*** the data to S3 and / or additional sources?
    * Have I tested every task individually?
    * Have I tested running the whole dag?
    * Do I have a comment starting with `#:` above every task?
    * Have I set all the run orders using `upstream` command?
    * Does my dag file have a docstring on top with a description of what it's for?
    * Do all my tasks have the following parameters:
        * `on_failure_callback=notify`
        * `on_retry_callback=notify`
        * `on_success_callback=notify`
    * Have I defined my dag timeinterval in `general.py`?
    * Am I uploading to S3 using the `S3FiletransferOperator`?
* Does my folder have one or more `_jobs.py` files where I define the jobs?
    * Do every one of my methods have a docstring (`"""`)?
    * Do my docstrings explain what the method does and what the parameters mean?
    * Am I doing enough logging to make debugging easier later on at failure points?
        * You should be using the `logging` package and calling `logging.info`.
        * All `_jobs` method calls should return a string with success.
    * Am I returning a message of success at the end of the method?
    * Are all of my `dtype` conversions set to `errors='coerce'` (ex: `pd.to_numeric(df['A'], errors='coerce')`) ([see issue](https://github.com/MrMaksimize/poseidon/issues/45))
* Have I added any necessary dependencies to `requirements.txt`
* General file checklist (all `.py` files)
    * Do I import any modules I don't use?
    * Am I putting all my connection parameters using the following pattern:
        * env.example = fake for documentation
        * local.env = for local environment
        * general.py = pull from env to make available to Python
        * call from `general.configs` dictionary to use them.
        * Reading files should be done using `general.file_to_string`
        * Writing csv files should be done using `general.pos_write_csv`


# DAG Guide
* Each part of the job should be its own task
* We should never store db creds in code.
* Use hooks and operators unless you have a good reason not to.
* We will only use `set_upstream` for task dependencies
* Paths and connection creds will be set using the `general.config` pattern
* Anything that is currently in `general` should be used instead of hard-coding the variable.

# Adhoc jobs
The `adhoc_jobs` directory contains dagless jobs.  These are there to be run manually.

# Data Runtime Management
* The `data` directory contains the following subdirs, with the following configurations:
    * `base`
        * used for referential data;
        * no directory per dataset (flat);
        * Committed to git;
    * `temp`
        * used for intra-dag data storage;
        * wipeable at any time;
        * directory per dataset;
        * Ignored by git;
    * `prod`
        * used as last-place storage before s3 transfer
        * wipeable at any time;
        * directory per dataset;
        * ignored by git;

# Documentation Management
The `docs` directory contains documentation.
It's automatically generated from the comments within the code.
For now, updated manually.

# Logs and debugging
There is a logs directory in poseidon but that's deprecated.  The entrypoint script creates a folder in docker at `/usr/local/airflow/logs` and that's where the logs are stored at runtime.  If you wish to see logs, you can either look in that folder or

`docker logs -f container_name`

# Dag Structure
* Imports
* Convenience variables
* Dag definition
* Task definitions, with doc strings
* Dependency definitions, using `set_upstream`


# S3 Management
* All buckets should be created in US-Standard Region
* Dev = local environments
* Stage = staging
* Prod = prod
* All buckets should be tagged with an env tag, and either dev, stage or prod
