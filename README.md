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
__Note__: [JLab Proxy Settings](https://github.com/JeffersonLab/jmyapi/wiki/JLab-Proxy)

__Note__: A copy of the deployments.properties file is stored in our internal ACE git repo under project __javacfg__.  Ideally we use a better strategy in the future such as Puppet to orchestrate deployments, or store it in a repository like Artifactory.

## API
[javadocs](https://jeffersonlab.github.io/jmyapi/)   

## Publishing Releases
To publish changes to our internal ACE artifactory repo ensure you have a gradle.properties file in your home directory with the following values:

```
artifactory_user=<user>
artifactory_password=<password>
```
Then execute:
```
gradlew artifactoryPublish
```
## See Also
   - [Download](https://github.com/JeffersonLab/jmyapi/releases)    
   - [MYAPI DOCS (Internal)](http://devweb.acc.jlab.org/controls_web/certified/myapi/)
   - [trippy](https://github.com/JeffersonLab/trippy)
