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
java -classpath build/libs/beam_part_i-0.1.jar io.exp.apachebeam.text.BeamPiRun --inputFile=./config/test/instruction.dat --output=/tmp/PiTest --runner=FlinkRunner --flinkMaster=localhost:9081 --filesToStage=build/libs/beam_part_i-0.1.jar
```