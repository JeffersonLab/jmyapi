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
## Manual Install
Download jmyapi jar [here](https://github.com/JeffersonLab/jmyapi/releases).  Now download mysql driver jar [here](https://mvnrepository.com/artifact/mysql/mysql-connector-java/5.1.42) and place in lib directory.  Create config/credentials.properites and config/deployments.properties files.

## API
[javadocs](https://jeffersonlab.github.io/jmyapi/)   

## See Also
   - [Download](https://github.com/JeffersonLab/jmyapi/releases)    
   - [MYAPI DOCS (Internal)](http://devweb.acc.jlab.org/controls_web/certified/myapi/)
