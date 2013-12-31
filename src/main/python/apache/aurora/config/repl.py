from twitter.aurora.config.loader import AuroraConfigLoader
from twitter.common.lang import Compatibility



import code
code.interact('Mesos Config REPL', 
    local=Compatibility.exec_function(AuroraConfigLoader.DEFAULT_SCHEMA, globals()))
