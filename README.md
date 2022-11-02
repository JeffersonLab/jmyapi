# jmyapi [![CI](https://github.com/JeffersonLab/jmyapi/actions/workflows/ci.yml/badge.svg)](https://github.com/JeffersonLab/jmyapi/actions/workflows/ci.yml) [![Maven Central](https://badgen.net/maven/v/maven-central/org.jlab/jmyapi)](https://repo1.maven.org/maven2/org/jlab/jmyapi/)
A Java client query API library for MYA (JLab's EPICS Archiver).  

---
- [Overview](https://github.com/JeffersonLab/jmyapi#overview)
- [Quick Start with Compose](https://github.com/JeffersonLab/jmyapi#quick-start-with-compose)   
- [Install](https://github.com/JeffersonLab/jmyapi#install)
- [API](https://github.com/JeffersonLab/jmyapi#api) 
- [Configure](https://github.com/JeffersonLab/jmyapi#configure) 
- [Build](https://github.com/JeffersonLab/jmyapi#build)
- [Test](https://github.com/JeffersonLab/jmyapi#test)
- [Release](https://github.com/JeffersonLab/jmyapi#release)
- [See Also](https://github.com/JeffersonLab/jmyapi#see-also)
---

## Overview
The Jeffeson Lab [EPICS](https://en.wikipedia.org/wiki/EPICS) archiver is named MYA, and was designed with command line and C++ APIs.  This library provides a native Java API for querying MYA.  If you are looking for a quick and easy way to obtain Archiver data see: [Public MYA Web Service](https://epicsweb.jlab.org/myquery/), which is built using this API plus [myquery](https://github.com/JeffersonLab/myquery).

## Quick Start with Compose
1. Grab project
```
git clone https://github.com/JeffersonLab/jmyapi
cd jmyapi
```
2. Launch Docker
```
docker compose up
```
3. Run hello app
```
gradlew hello
```

## Install
The library requires a Java 8+ JVM and standard library at run time, plus has a depenency on the MariaDB database driver.  

You can obtain the jmyapi jar file from the [Maven Central repository](https://repo1.maven.org/maven2/org/jlab/jmyapi/) directly or from a Maven friendly build tool with the following coordinates (Gradle example shown):
```
implementation 'org.jlab:jmyapi:<version>'
```
Check the [Release Notes](https://github.com/JeffersonLab/jmyapi/releases) to see what has changed in each version.  

## API
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

## Configure
### Properties
The library requires configuration properties be included in the runtime classpath.  Specifically a _deployments.properties_ file is needed to indicate the host names of MYA servers.  A template for the properties can be found [here](https://github.com/JeffersonLab/jmyapi/blob/main/config/deployments.properties.template).   You must create your own _deployments.properties_ file and include it on the runtime classpath.

### Authentication
In order to interact with the MYA server users must authenticate (MySQL user auth).  This is done using a username and password from a _credentials.properties_ file, which must be included in the runtime classpath.  A template for the properties can be found [here](https://github.com/JeffersonLab/jmyapi/blob/main/config/credentials.properties.template).


## Build
This project is built with [Java 17](https://adoptium.net/) (compiled to Java 8 bytecode), and uses the [Gradle 7](https://gradle.org/) build tool to automatically download dependencies and build the project from source:

```
git clone https://github.com/JeffersonLab/jmyapi
cd jmyapi
gradlew build
```
**Note**: If you do not already have Gradle installed, it will be installed automatically by the wrapper script included in the source

**Note**: Jefferson Lab has an intercepting [proxy](https://gist.github.com/slominskir/92c25a033db93a90184a5994e71d0b78)

## Test
The unit tests run automatically upon build.   Integration tests are separate and require docker compose environment (MariaDB database).
1. Launch Docker:
```
docker compose up
```
2. Run integration tests:
```
gradlew integrationTest
```

## Release
1. Run the integration tests to ensure code is good.
2. Bump the version number and release date in the build.gradle file and commit and push to GitHub (using [Semantic Versioning](https://semver.org/)).
3. Create a new release on the GitHub [Releases](https://github.com/JeffersonLab/jmyapi/releases) page corresponding to the version in build.gradle (Enumerate changes and link issues). 
4. Publish new artifact to maven central with:
```
gradlew publishToSonatype closeAndReleaseSonatypeStagingRepository
```
5. Update javadocs by copying them from build dir into gh-pages branch and updating index.html (commit, push).

## See Also
   - [Developer Notes](https://github.com/JeffersonLab/jmyapi/wiki/Developer-Notes)
   - [myquery](https://github.com/JeffersonLab/myquery)  
   - [trippy](https://github.com/JeffersonLab/trippy)
