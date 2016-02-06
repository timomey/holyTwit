# tagger web service

please see application.conf to update configs
	
	
## running 

	ssh ubuntu@

	mvn install -Dmaven.test.skip=true
	
	# please see application.conf to update configs
	
	
	# run as webservice (tagger service only)
	nohup java -Dspring.application.json='{"tagger.webservice":"true"}'   -jar target/TAG-0.0.7-SNAPSHOT.jar service  & 

# curl -H "Accept: application/json" -H "Content-type: application/json" -X POST -d '{"668158130128220160":"test"}' http://localhost:8555/tagbatch
	
