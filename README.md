# Local Docker Pandas Pipeline 
Pandas docker test to create idempotent pipelines locally (on Windows)

## Setup
Using an existing docker container with pandas to execute script locally
```sh
git clone https://github.com/Tgordon523/pipeline_docker_test.git
cd pipeline_docker_test
docker pull amancevice/pandas
docker run -it -v ${pwd}:/var/lib/pandas amancevice/pandas sh
```

## Inside the docker container 
Replace date paramaters to retrieve data for given date
```sh
pip install pyarrow fastparquet requests 
python data_pipeline_inspections.py --date YYYY-MM-DD
```