FROM python

RUN pip install pandas
RUN pip install pyarrow

WORKDIR /app
COPY pipeline.py pipeline.py

ENTRYPOINT [ "python", "pipeline.py" ]