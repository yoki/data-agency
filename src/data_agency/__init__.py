import re
import data_agency.commands.load.load_service as load_service

import data_agency.magic as magic


def load_ipython_extension(ipython):
    """
    This function is called when the extension is loaded.
    It registers the magic functions with the IPython shell.
    """
    magic.load_ipython_extension(ipython)


def load(args):
    # needed for using data from python code direclty
    return load_service.load(args)
