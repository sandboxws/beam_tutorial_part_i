FROM openjdk:8-jre-alpine

# Install requirements
RUN apk add --no-cache bash snappy libc6-compat

ENV LD_LIBRARY_PATH /lib64

# Flink environment variables
ENV FLINK_INSTALL_PATH /opt
ENV FLINK_HOME /opt/flink
ENV FLINK_JOB ${FLINK_INSTALL_PATH}/JOB

RUN mkdir -p $FLINK_HOME
RUN mkdir -p $FLINK_JOB
# flink-dist can point to a directory or a tarball on the local system
ARG job_jar=NOT_SET

# Install build dependencies and flink
ADD $job_jar ${FLINK_JOB}/job.jar

COPY runflinktext.sh ${FLINK_JOB}
RUN chmod a+x ${FLINK_JOB}/runflinktext.sh

RUN addgroup -S flink && adduser -D -S -H -G flink -h $FLINK_HOME flink && \
  chown -R flink:flink ${FLINK_JOB}/job.jar && \
  chown -R flink:flink ${FLINK_JOB}/runflinktext.sh && \
  chown -h flink:flink ${FLINK_HOME}

COPY software/peer2peer-fc16866f48fd.json /
ENV GOOGLE_APPLICATION_CREDENTIALS=/peer2peer-fc16866f48fd.json

USER flink

WORKDIR ${FLINK_JOB}


# ENTRYPOINT ["initial script"]
#CMD ["java","-classpath","job.jar","io.exp.apachebeam.text.BeamPiRun", \
#     "--runner=FlinkRunner","--flinkMaster=${JOB_MANAGER_RPC_ADDRESS}:${JOB_MANAGER_RPC_PORT}", \
#     "--inputFile=gs://pi_calculation/instruction.dat","--output=gs://pi_calculation/piDtest", \
#     "--filesToStage=job.jar","--maxBundleSize=200"]
CMD ["./runflinktext.sh"]