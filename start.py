"""
Authors:
1) Joel Marvin Tellis (jt4680)
2) Sahil Bakshi (sb8916)
"""
import logging
from pathlib import Path
import plac

from RepCRec.config import config
from RepCRec.SiteManager import SiteManager
from RepCRec.TransactionManager import TransactionManager
from RepCRec.Simulator import Simulator

class RepCRec:
    """
    RepCRec class representing the whole project.
    Job is to start site manager, transaction manager and Simulator.

    Parameters:
        file_path: File containing instructions
        num_sites: Number of sites
        num_variables: Number of variables
        out_file: If out_file is present, logs will be written to it

    Returns:
        The output of the test case
    """
    @plac.annotations(
        file_path=("Input file path.","positional", None, str),
        num_sites=("Number of Sites", "option", "n", int),
        num_variables=("Number of variables", "option", "v", int),
        out_file=("Output file, if not passed by default output will be printed to std output", "option", "o", str))
    def __init__(self, file_path, num_sites=config['NUM_SITES'],
                 num_variables=config['NUM_VARIABLES'],
                 out_file=None):
        p = Path('.')
        p = p / file_path

        if out_file:
            open(out_file, 'w', encoding='UTF-8').close()

        logging.basicConfig(filename=out_file,
                            filemode='w',
                            encoding='utf-8',
                            format='%(levelname)s - %(message)s',
                            level=config['LOG_LEVEL'])

        self.site_manager = SiteManager(num_sites, num_variables)

        self.transaction_manager = TransactionManager(num_variables, num_sites, self.site_manager)

        self.simulator = Simulator(p, self.site_manager, self.transaction_manager)

    def run(self):
        """
        Start simulator
        """
        self.simulator.run()


if __name__ == "__main__":
    main = plac.call(RepCRec)
    main.run()
