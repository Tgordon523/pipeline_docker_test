FROM python:3.11.1-slim

WORKDIR /

COPY requirements.txt data_pipeline_inspections.py ./

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "data_pipeline_inspections.py", "--date"]
CMD ["2023-09-07"]
