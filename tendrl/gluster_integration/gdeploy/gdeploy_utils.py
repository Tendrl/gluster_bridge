import os
import uuid

from tendrl.commons.utils import cmd_utils

GDEPLOY_CONFIG_PATH = "/var/run/tendrl/gdeploy/config_"

def add_section(section):
    return "[" + section + "]\n"

def cook_gdeploy_config(recipe):
    config_str = ""
    for el in recipe:
        k = el.keys()[0]
        v = el.values()[0]
        config_str += add_section(k)
        if type(v) == list:
            for el in v:
                config_str += el + "\n"
        elif type(v) == dict:
            for var, val in v.iteritems():
                if val == None:
                    line = val
                    continue
                line = var + "=" + (str(val) if type(val) != list else ",".join(val))
                config_str += line + "\n"
        config_str += "\n"

    return config_str
                
def invoke_gdeploy(config):
    conf_file = GDEPLOY_CONFIG_PATH + str(uuid.uuid4()) + ".conf"
    with open(conf_file, 'w') as f:
        f.write(config)
    cmd = cmd_utils.Command("gdeploy -c " + conf_file)
    # to be changed to correct path
    out, err, rc = cmd.run("/var/run/tendrl/tendrl_exe")
    os.remove(conf_file)
    return out, err, rc
    
    
