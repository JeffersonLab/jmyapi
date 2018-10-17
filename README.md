# jmyapi
A Java client query API library for MYA (JLab's EPICS Archiver).  

## Automated Install
Use Gradle build to download dependencies:
```
git clone https://github.com/JeffersonLab/jmyapi
cd jmyapi
gradlew build
gradlew -Dorg.gradle.daemon=false config
vi config/deployments.properties
gradlew hello
```
__Note__: If you are behind a firewall with a proxy server you might need to setup proxy settings.  For git clone to work at Jefferson Lab on an ACE Linux workstation with a tcsh shell you could execute:

```
setenv https_proxy jprox.jlab.org:8081
```

Now for gradlew build to work it executes Java, which needs to (1) connect to Internet and (2) trust SSL certificates.  At Jefferson Lab on a Linux workstation you could execute:

```
gradlew -Dhttps.proxyHost=jprox.jlab.org -Dhttps.proxyPort=8081 -Djavax.net.ssl.trustStore=/etc/pki/ca-trust/extracted/java/cacerts build
```

__Note__: A copy of the deployments.properties file is stored in our internal ACE git repo under project __javacfg__.  Ideally we use a better strategy in the future such as Puppet to orchestrate deployments, or store it in a repository like Artifactory.
## Manual Install
Download jmyapi jar [here](https://github.com/JeffersonLab/jmyapi/releases).  Now download mysql driver jar [here](https://mvnrepository.com/artifact/mysql/mysql-connector-java/5.1.42) and place in lib directory.  Create config/credentials.properites and config/deployments.properties files.

## API
[javadocs](https://jeffersonlab.github.io/jmyapi/)   

## See Also
   - [Download](https://github.com/JeffersonLab/jmyapi/releases)    
   - [MYAPI DOCS (Internal)](http://devweb.acc.jlab.org/controls_web/certified/myapi/)
