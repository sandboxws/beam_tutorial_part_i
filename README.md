## Run inmemory
Run command:
```
java -classpath beam_part_i-0.1.jar io.exp.apachebeam.inmemory.BeamPiRun
```

## Direct Runner : Run Text file as I/O
Build command:
```
gradle -Pdirect clean build
```
Run command:
```
java -classpath build/libs/beam_part_i-0.1.jar io.exp.apachebeam.text.BeamPiRun --inputFile=./config/test/instruction.dat --output=/tmp/PiTest
```

## Flink Runner : Run Text file as I/O
Build command:
```
gradle -Pflink clean build
```
Run command:
```
java -classpath build/libs/beam_part_i-0.1.jar io.exp.apachebeam.text.BeamPiRun \
--runner=FlinkRunner --flinkMaster=localhost:9081 \
--inputFile=/Users/dexter/sandbox/apachebeam/beam_tutorial_part_i/config/test/instruction.dat \
--output=/tmp/PiTest \
--filesToStage=/Users/dexter/sandbox/apachebeam/beam_tutorial_part_i/build/libs/beam_part_i-0.1.jar \
--parallelism=2 \
--maxBundleSize=1000
```

## Direct Runner : Run Kafka as I/O
Build command:
```
gradle -Pdirect clean build
```
Run command:
```
java -classpath build/libs/beam_part_i-0.1.jar io.exp.apachebeam.text.BeamPiRun --inputTopic=pi --outputTopic=pi_out --output=/tmp/PiTest
```

## Flink Runner : Run Kafka as I/O
Build command:
```
gradle -Pflink clean build
```
Run command:
```
Local:
java -classpath build/libs/beam_part_i-0.1.jar io.exp.apachebeam.kafka.BeamPiRun \
--runner=FlinkRunner \
--flinkMaster=localhost:9081 \
--bootStrapServer=localhost:9092 \
--inputTopic=pi \
--outputTopic=pi_out \
--output=/tmp/PiKafkaFlink \
--filesToStage=/Users/dexter/sandbox/apachebeam/beam_tutorial_part_i/build/libs/beam_part_i-0.1.jar \
--parallelism=2

Kafka container:
java -classpath build/libs/beam_part_i-0.1.jar io.exp.apachebeam.kafka.BeamPiRun \
--runner=FlinkRunner \
--flinkMaster=localhost:9081 \
--bootStrapServer=192.168.99.107:9094 \
--inputTopic=pi \
--outputTopic=pi_out \
--output=/tmp/PiKafkaFlink \
--filesToStage=/Users/dexter/sandbox/apachebeam/beam_tutorial_part_i/build/libs/beam_part_i-0.1.jar \
--parallelism=2
```

# Flink docker run
docker compose directory:
/Users/dexter/sandbox/DockerTrain/flink-session/docker-compose.yml
docker-compose up
docker-compose kill

