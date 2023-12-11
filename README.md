# Local Docker Pandas Pipeline 
Pandas docker test to create idempotent pipelines locally (on Windows)

## Setup
Build docker image to run idemptotent pipeline 

```sh
docker build -t data-pipeline .
```

## Run 
In the same directory, open command line as adminstrator to test the pipeline. 
The default date is 2023-09-07 and can be adjusted if a valid date is entered after the name of the docker image. 

```sh
docker run -v ${PWD}:/pipeline_docker_test data-pipeline #date YYYY-MM-DD
```
