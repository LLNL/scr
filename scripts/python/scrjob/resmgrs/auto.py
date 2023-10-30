"""AutoResourceManager is called to return the appropriate ResourceManager
class.

This is the class called to obtain an instance of a resource manager.

To add a new resource manager:
* Insert the new class name at the end of scrjob.resmgrs import statement
* Insert the condition and return statement for your new resource manager within __new__

The resmgr string normally comes from the value of config.SCR_RESOURCE_MANAGER
"""

from scrjob import config

from scrjob.resmgrs import (
    ResourceManager,
    LSF,
    PBSALPS,
    #PMIX,
    SLURM,
    FLUX,
)


class AutoResourceManager:

    def __new__(cls, resmgr=None):
        # see if we are in a flux instance
        try:
            fluxresmgr = FLUX()
            return fluxresmgr
        # not in a flux instance, just continue
        except:
            pass

        if resmgr is None:
            resmgr = config.SCR_RESOURCE_MANAGER

        if resmgr == 'SLURM':
            return SLURM()
        if resmgr == 'LSF':
            return LSF()
        if resmgr == 'APRUN':
            return APRUN()
        #if resmgr=='PMIX':
        #  return PMIX()

        return ResourceManager()


if __name__ == '__main__':
    resourcemgr = AutoResourceManager()
    print(type(resourcemgr))
