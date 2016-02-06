from jq import jq
import subprocess

# cat chorea.json | jq '.drugs[0].images[].labeler'
f = open('/home/ubuntu/conditions/chorea.json', 'r+')
contents = f.read()


#subprocess.call("cat chorea.json | jq '.drugs[0].name_generic'", shell=True)



def run_jq_query_resulting_in_array(query):
	out = subprocess.Popen(query, shell=True, stdout=subprocess.PIPE).stdout.read()
	if (out=="null\n"):
		return []
	drug_companies = subprocess.Popen(query, shell=True, stdout=subprocess.PIPE).stdout.read().replace('"','').replace('null',None).split("\n")
	drug_companies = filter(None,list(set(drug_companies))) 
	return drug_companies

def run_jq_query_resulting_in_string(query):
	drug_name = subprocess.Popen(query, shell=True, stdout=subprocess.PIPE).stdout.read().replace('"','').replace('\n','')
	return drug_name


drug_name = run_jq_query_resulting_in_string("cat chorea.json | jq '.drugs[0].name';")
drug_companies = run_jq_query_resulting_in_array("cat chorea.json | jq '.drugs[0].images[].labeler'")

symptoms = run_jq_query_resulting_in_array("cat chorea.json | jq '.condition.symptoms';")


def save_all_iodine_dot_com_symptoms_as_list():






