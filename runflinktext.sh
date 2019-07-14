#!/bin/sh
java -classpath job.jar io.exp.apachebeam.text.BeamPiRun \
--runner=FlinkRunner --flinkMaster=${JOB_MANAGER_RPC_ADDRESS}:${JOB_MANAGER_RPC_PORT} \
--inputFile=gs://pi_calculation/instruction.dat --output=gs://pi_calculation/piDtest \
--filesToStage=job.jar \
--maxBundleSize=200