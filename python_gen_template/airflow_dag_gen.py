#Import necessary functions from Jinja2 module
from jinja2 import Environment, FileSystemLoader

#Import YAML module
import yaml

# Load data from YAML into Python dictionary
config_data = yaml.full_load(open('./ent_cbs_ad_summary_config.yml'))

# Load templates file from templtes folder
env = Environment(loader = FileSystemLoader('.'), trim_blocks=True, lstrip_blocks=False)
template = env.get_template('bq_summary_dag_template.py')

#Render the template with data and print the output
with open('final_bq_summary_dag.py', 'w') as f:
    f.write(template.render(config_data))
