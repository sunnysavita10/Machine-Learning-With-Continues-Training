FROM python:3.8
USER root
RUN mkdir /app
COPY . /app/
WORKDIR /app/
RUN pip3 install -r requirements.txt
ENV AIRFLOW_HOME="/app/airflow"
ENV AIRFLOW_CORE_DAGBAG_IMPORT_TIMEOUT=1000
ENV AIRFLOW_CORE_ENABLE_XCOM_PICKLING=True
RUN airflow db init
RUN airflow users create -e sunny.savita@ineuron.ai -f sunny -l savita -p admin -r Admin -u admin
RUN chmod 777 start.sh
RUN apt update -y 
ENTRYPOINT [ "/bin/sh" ]
CMD ["start.sh"]