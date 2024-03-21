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
The Jeffeson Lab [EPICS](https://en.wikipedia.org/wiki/EPICS) archiver is named MYA, and was designed with command line and C++ APIs.  This library provides a native Java API for querying MYA.  If you are looking for a quick and easy way to obtain MYA data see: [Public MYA Web Service](https://epicsweb.jlab.org/myquery/), which is built using this API plus [myquery](https://github.com/JeffersonLab/myquery).

## Quick Start with Compose
1. Grab project
```
git clone https://github.com/JeffersonLab/jmyapi
cd jmyapi
```
2. Launch [Compose](https://github.com/docker/compose)
```
docker compose up
```
3. Run hello app
```
gradlew hello
```

## Install
The library requires a Java 8+ JVM and standard library at run time, plus has a depenency on the MariaDB database driver.  

You can obtain the jmyapi jar file from the [Maven Central repository](https://repo1.maven.org/maven2/org/jlab/jmyapi/) (or [Sonatype Backing Store](https://s01.oss.sonatype.org/content/repositories/releases/org/jlab/jmyapi/)) directly or from a Maven friendly build tool with the following coordinates (Gradle example shown):
```
implementation 'org.jlab:jmyapi:<version>'
```
Check the [Release Notes](https://github.com/JeffersonLab/jmyapi/releases) to see what has changed in each version.  

## API
   - [Javadocs](https://jeffersonlab.github.io/jmyapi/)

### Example
https://github.com/JeffersonLab/jmyapi/blob/36e9f0b55b9707f7d910ca0deb73e377153117bc/src/test/java/org/jlab/mya/HelloWorld.java#L27-L47

## Configure
### Deployments
A _deployments.properties_ file must be included in the runtime classpath to indicate the host names of MYA servers.  A template for the properties can be found [here](https://github.com/JeffersonLab/jmyapi/blob/main/config/deployments.properties.template).

### Credentials
A _credentials.properties_ file must be included in the runtime classpath to indicate the MariaDB username and password to use.  A template for the properties can be found [here](https://github.com/JeffersonLab/jmyapi/blob/main/config/credentials.properties.template).

**Note**: An alternative is to use a JNDI DataSource via the [PooledNexus](https://github.com/JeffersonLab/jmyapi/blob/f4f27b9e1cb7c4430d467d409cdf530d2c4aa8ac/src/main/java/org/jlab/mya/nexus/PooledNexus.java#L21) instead of using the OnDemandNexus with a credentials file.  A PooledNexus is often used with an application server such as Tomcat.  See [DataSource Notes](https://github.com/JeffersonLab/jmyapi/wiki/Developer-Notes#datasource-notes).

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
1. Bump the version number in the VERSION file and commit and push to GitHub (using [Semantic Versioning](https://semver.org/)).
2. The [CD](https://github.com/JeffersonLab/jmyapi/blob/main/.github/workflows/cd.yml) GitHub Action should run automatically to tag the source and create release notes summarizing any pull requests.   Edit the release notes to add any missing details.
3. The [Publish artifact](https://github.com/JeffersonLab/java-workflows/blob/main/.github/workflows/maven-publish.yml) GitHub Action should run automatically to create a deployment artifact on maven central.
4. The [Publish docs](https://github.com/JeffersonLab/java-workflows/blob/main/.github/workflows/gh-pages-publish.yml) GitHub Action should run automatically to create javadocs.

## See Also
   - [Developer Notes](https://github.com/JeffersonLab/jmyapi/wiki/Developer-Notes)
   - [myquery](https://github.com/JeffersonLab/myquery)  
   - [trippy](https://github.com/JeffersonLab/trippy)
