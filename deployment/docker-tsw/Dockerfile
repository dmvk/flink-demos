FROM flink:1.15-SNAPSHOT
RUN mkdir /opt/flink/usrlib
RUN mkdir /opt/flink/plugins/s3
RUN cp /opt/flink/opt/flink-s3-fs-presto-1.15-SNAPSHOT.jar /opt/flink/plugins/s3
#RUN cp /opt/flink/examples/streaming/TopSpeedWindowing.jar /opt/flink/usrlib
COPY failing-all.jar /opt/flink/usrlib
RUN rm -r /opt/flink/opt
RUN rm -r /opt/flink/examples
