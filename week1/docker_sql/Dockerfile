FROM python:3.9

RUN apt-get install wget 
RUN pip install pandas sqlalchemy psycopg2 pyarrow

# working directory is the location in the image to where the 
# file will copied from the local directory
WORKDIR /app
COPY ingest_data.py ingest_data.py

# the default ENTRYPOINT in the image python:3.9 is the python terminal 
# here we override this default configuration so that the image starts
# with a bash terminal when executed with -it mode
ENTRYPOINT [ "python","ingest_data.py" ]