# jmyapi [![CI](https://github.com/JeffersonLab/jmyapi/actions/workflows/ci.yml/badge.svg)](https://github.com/JeffersonLab/jmyapi/actions/workflows/ci.yml) [![Maven Central](https://badgen.net/maven/v/maven-central/org.jlab/jmyapi)](https://repo1.maven.org/maven2/org/jlab/jmyapi/)
A Java client query API library for MYA (JLab's EPICS Archiver).  

If you are looking for a quick and easy way to obtain Archiver data see: [Public MYA Web Service](https://epicsweb.jlab.org/myquery/), which is built using this API plus [myquery](https://github.com/JeffersonLab/myquery).

---
   - [Usage](https://github.com/JeffersonLab/jmyapi#usage)
     - [API](https://github.com/JeffersonLab/jmyapi#api) 
     - [Example](https://github.com/JeffersonLab/jmyapi#example) 
     - [Configure](https://github.com/JeffersonLab/jmyapi#configure) 
   - [Build](https://github.com/JeffersonLab/jmyapi#build)
- [See Also](https://github.com/JeffersonLab/jmyapi#see-also)
---

## Usage
The library is a single jar file plus a dependency on the MySQL database driver jar and the Java 8+ JVM and standard library.  You can obtain the jmyapi jar file from the [Maven Central repository](https://repo1.maven.org/maven2/org/jlab/jmyapi/) directly or from a Maven friendly build tool with the following coordinates (Gradle example shown):
```
implementation 'org.jlab:jmyapi:6.1.0'
```
You can check the [Release Notes](https://github.com/JeffersonLab/jmyapi/releases) to see what has changed in each version.  

### API
   - [Javadocs](https://jeffersonlab.github.io/jmyapi/)

### Example
```
        DataNexus nexus = new OnDemandNexus("ops");

        String pv = "R123PMES";
        Instant begin = TimeUtil.toLocalDT("2017-01-01T00:00:00.123456");
        Instant end = TimeUtil.toLocalDT("2017-01-01T00:01:00.123456");

        Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);
        try (EventStream<FloatEvent> stream = nexus.openEventStream(metadata, begin, end)) {

            FloatEvent event;

            while ((event = stream.read()) != null) {
                System.out.println(event.toString(6));
            }
        }
```

### Configure
#### Properties
The library requires configuration properties be included in the runtime classpath.  Specifically a _deployments.properties_ file is needed to indicate the host names of MYA servers.  A template for the properties can be found [here](https://github.com/JeffersonLab/jmyapi/blob/main/config/deployments.properties.template).   You must create your own _deployments.properties_ file and include it on the runtime classpath.

#### Authentication
In order to interact with the MYA server users must authenticate (MySQL user auth).  This is done using a username and password from a _credentials.properties_ file, which must be included in the runtime classpath.  A template for the properties can be found [here](https://github.com/JeffersonLab/jmyapi/blob/main/config/credentials.properties.template).


### Build
This [Java 8+](https://adoptopenjdk.net/) project uses the [Gradle 6](https://gradle.org/) build tool to automatically download dependencies and build the project from source:

```
git clone https://github.com/JeffersonLab/jmyapi
cd jmyapi
gradlew build
```
**Note**: If you do not already have Gradle installed, it will be installed automatically by the wrapper script included in the source

**Note**: Jefferson Lab has an intercepting [proxy](https://gist.github.com/slominskir/92c25a033db93a90184a5994e71d0b78)

## See Also
   - [Developer Notes](https://github.com/JeffersonLab/jmyapi/wiki/Developer-Notes)
   - [myquery](https://github.com/JeffersonLab/myquery)  
   - [trippy](https://github.com/JeffersonLab/trippy)
