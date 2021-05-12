# Nuix exercise project

## How to Build
Once you clone the repository, cd into the project root directory and run the following command to build the project:
```
mvn clean install
```

## How to Run
Once you build the project, the build command will generate a .jar file in the target/ directory. The name of the .jar file will be exercise-0.0.1-SNAPSHOT.jar. You can run this project by executing the following command from the project root directory:
```
java -jar target/exercise-0.0.1-SNAPSHOT.jar
```

In this case all zip files from S3 bucket will be downloaded and processed.

Running program with arguments (arguments are zip file names to download from S3 bucket):
```
java -jar target/exercise-0.0.1-SNAPSHOT.jar zipfile1 zipfile2
```
E.g.:
```
java -jar target/exercise-0.0.1-SNAPSHOT.jar data.zip
```