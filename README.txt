```
./start-build-env.sh

# build mrbench
cd hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient
sudo mvn package -DskipTests -Dmaven.javadoc.skip=true

# build teragen/terasort
cd hadoop-mapreduce-project/hadoop-mapreduce-examples
sudo mvn package -DskipTests -Dmaven.javadoc.skip=true
```
