# jmyapi
A Java client query API library for MYA (JLab's EPICS Archiver).  

If you are looking for a quick and easy way to obtain Archiver data see: [myquery](https://github.com/JeffersonLab/myquery), which is built using this API.

## Automated Install
Use Gradle to download dependencies:
```
git clone https://github.com/JeffersonLab/jmyapi
cd jmyapi
gradlew build -x test
gradlew -Dorg.gradle.daemon=false config
vi config/deployments.properties
gradlew hello
```
__Note__: Gradle can't download dependencies if you're behind a firewall - [JLab Proxy Settings](https://github.com/JeffersonLab/jmyapi/wiki/JLab-Proxy)

__Note__: A copy of the deployments.properties file is stored in our internal ACE git repo under project __javacfg__.  Alternatively check out myaweb.acc.jlab.org.   You can obtain database credentials from a Mya Admin.

__Note__: The client query API requires a connection the Mya MySQL databases.  If you are behind a firewall you can setup tunnels with a tool such as PuTTy.   The deployments.properties template has examples of configuring proxy ports.

## API Docs (with Examples)
[javadocs](https://jeffersonlab.github.io/jmyapi/)   

## See Also
   - [myquery](https://github.com/JeffersonLab/myquery)
   - [Download](https://github.com/JeffersonLab/jmyapi/releases)    
   - [Publish New Release](https://github.com/JeffersonLab/jmyapi/wiki/Publish-New-Releases-to-Artifactory)
   - [MYAPI DOCS (Internal)](http://devweb.acc.jlab.org/controls_web/certified/myapi/)
   - [trippy](https://github.com/JeffersonLab/trippy)
