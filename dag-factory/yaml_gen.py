#Import necessary functions from Jinja2 module
from jinja2 import Environment, FileSystemLoader

#Import YAML module
import yaml
from ruamel.yaml import YAML


#Load data from YAML into Python dictionary
config_data = yaml.full_load(open('./ent_cbs_ad_summary_config.yml'))

#Load Jinja2 template
jinja_yaml = YAML()
jinja_yaml.allow_duplicate_keys = True
jinja_yaml.preserve_quotes=True
# jinja_yaml.explicit_start = True
yaml_content = jinja_yaml.load(Environment(loader = FileSystemLoader('.')).get_template('bq_summary_factory_template.yml').render(config_data))
print(yaml_content)

#Render the template with data and print the output
with open('final_dag_factory.yml', 'w') as f:
    output_yaml = jinja_yaml.dump(yaml_content, f)
#print(output_yaml)
