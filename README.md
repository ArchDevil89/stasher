###Stasher

Simple logging tool, it's forwarding docker logs to logstash - http://logstash.net/

#### Building D tools

First you need D Compiler and Dub build tool.

```sh
# D Compiler
http://downloads.dlang.org/releases/2014/dmd_2.066.0-0_amd64.deb
# Same stuff in a zip
http://downloads.dlang.org/releases/2014/dmd.2.066.0.zip

# Dub stand-alone binary
http://code.dlang.org/files/dub-0.9.22-linux-x86_64.tar.gz
```

####Usage
Config example(logs.conf)
```
input { 
	tcp {
		codec => json_lines {
      			charset => "UTF-8"} 
                port => 12345 
	} 
} 
output { elasticsearch { host  => "elogs1" protocol => "http"} }
```
Running logstash
```
./bin/logstash -f logs.conf
```
You may need root priveleges to run stasher
```
stasher path_to_docker_containers logstash_host port
#example
stasher /var/lib/docker/containers localhost 12345
```