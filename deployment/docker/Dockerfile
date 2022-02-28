FROM flink:1.15-slim
RUN mkdir /opt/flink/usrlib
RUN mkdir /opt/flink/plugins/s3
COPY --from=flink:1.15 /opt/flink/opt/flink-s3-fs-presto-1.15-SNAPSHOT.jar /opt/flink/plugins/s3
COPY --from=flink:1.15 /opt/flink/examples/streaming/TopSpeedWindowing.jar /opt/flink/usrlib
COPY very-heavy-flink-job-1.0-SNAPSHOT.jar /opt/flink/usrlib
