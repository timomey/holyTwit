#!/usr/bin/env python
import sys

if __name__ == '__main__':
	cluster_aws_name = str(sys.argv[1])
	ssh_config_name = str(sys.argv[2])
	
	with open('./tmp/'+cluster_aws_name+'/public_dns','r') as public_dns:
		with open('/Users/timomeyer/.ssh/config','a') as ssh_conf:
			for i, l in enumerate(public_dns):
				ssh_conf.write('Host '+ssh_config_name+str(i+1)+'\n')
				ssh_conf.write('  HostName '+l)	
				ssh_conf.write('  User ubuntu\n')
				ssh_conf.write('  IdentityFile ~/.ssh/timo-meyer.pem'+'\n\n')
