"""
Authors:
1) Joel Marvin Tellis (jt4680)
2) Sahil Bakshi (sb8916)
"""
import logging
from collections import defaultdict

from RepCRec.Site import Site
from RepCRec.constants import FAIL_FUNC, DUMP_FUNC, RECOVER_FUNC
from RepCRec.enums.TransactionStatus import TransactionStatus

log = logging.getLogger(__name__)


class SiteManager:
    """
    Site Manager manages all the sites and the instructions related to them.
    It is the central point of contact for transaction manager for queries related to site.

    Paramters:
        num_sites: Number of sites
        num_variables: Number of total variables present

    Attributes:
        num_sites: Number of sites
        num_variables: Number of total variables present
        sites_list : List of all Site objects
        site_failure_history ( Dict() ) : KEY is site_id and Value is list of timestamps when the site failed
        site_recover_history ( Dict() ) : KEY is site_id and Value as list of timestamps when the site recovered
        waiting_txn ( Dict( List[] ) ) : KEY as site_id and Values as tuple of (Transaction obj, variable_id) waiting for a read on
                        odd indexed varaible because the site was down
        waiting_txn_even_var ( Dict( Dict() ) : KEY as site_id and inner dict has INNER KEY = variable_id and Values txn obj
                        waiting for a read on even indexed varaible because the site was down
        current_time (int) : The global time at this point
    """

    def __init__(self, num_sites, num_variables):
        # Append None on zero index for easy retreival
        self.num_sites = num_sites
        self.sites_list = [None] + [Site(i) for i in range(1, num_sites + 1)]
        self.site_failure_history = dict()
        self.site_recover_history = dict()
        for i in range(1, num_sites + 1):
            self.site_failure_history[i] = [0]
            self.site_recover_history[i] = [float('inf')]
        self.num_variables = num_variables
        self.waiting_txn = defaultdict(list)
        def temp_dict():
            return defaultdict(list)
        self.waiting_txn_even_var = defaultdict(temp_dict)
        self.current_time = 0

    def process_instr(self, current_time, instruction):
        """
        Simulator calls this function when the Instruction has to deal with sites.
        This includes instructions for fail, recover and dump.

        Parameters:
            current_time : The global time at this point
            instruction : object of class Instruction, contains the current instruction attributes
        """
        params = instruction.get_params()

        self.current_time = current_time

        if instruction.get_instruction_type() == DUMP_FUNC:
            # DUMP
            log.info("Site DUMP from SiteManager")
            self.dump()
        elif instruction.get_instruction_type() == FAIL_FUNC:
            # Bring a site down
            self.fail_site(int(params[0]))
        elif instruction.get_instruction_type() == RECOVER_FUNC:
            # Bring a site UP
            self.recover_site(int(params[0]))
        return

    def dump(self):
        """
        Print the dump of all sites
        """
        for i in range(1, self.num_sites+1):
            site = self.sites_list[i]
            print("\nSite "+str(i)+" -", end = " ")
            for key, value in site.get_data_manager().get_committed_variables().items():
                print("x"+str(key)+" : "+str(value.get_value()), end=", ")
            print()
        print()
        return

    def get_site(self, index):
        """
        Returns a site on particular index
        Parameters:
            index: Index of the site to be returned
        Returns:
            Site object present at index passed
        """
        return self.sites_list[index]

    def get_all_sites(self):
        """
        Returns the list of all sites maintained

        Returns:
            List of Site object
        """
        return self.sites_list[1:]

    def add_wait_txn(self, site_id, transaction, variable_id):
        """
        Adds a Txn to wait list until the site recovers for odd indexed variables

        Parameters:
            site_id: ID of the site
            variable_id : ID of the variable
            transaction : Transaction Object
        """
        self.waiting_txn[site_id].append([transaction, variable_id])

    def add_wait_txn_even(self, site_id, transaction, variable_id):
        """
        Adds a Txn to wait list of even indexed variables until the site recovers

        Parameters:
            site_id: ID of the site
            variable_id : ID of the variable
            transaction : Transaction Object
        """
        self.waiting_txn_even_var[site_id][variable_id].append(transaction)

    def fail_site(self, index):
        """
        Fail a particular site

        Parameters:
            index: Index of the site to be failed
        """
        log.info("Site %s failed",str(index))
        self.sites_list[index].fail()
        self.site_failure_history[index].append(self.current_time)

    def recover_site(self, index):
        """
        Recover a particular site

        Parameters:
            index: Index of the site to be recovered
        """
        log.info("Site %s recovered",str(index))
        self.sites_list[index].recover()
        self.site_recover_history[index].append(self.current_time)

        # Even Indexed Variables
        pending_txns = self.waiting_txn_even_var[index]

        cleared_variables = []

        for var_id, txn_obj_list in pending_txns.items() :
            log.info("Executing Pending reads (if any) for even indexed variables as site recovered")
            for txn in txn_obj_list :
                if txn.get_status() == TransactionStatus.WAITING and not(txn.get_status() == TransactionStatus.ABORTED) :
                    log.info("Txn %s : Reading  x%s from Site %s", txn.get_name(), str(var_id), str(index))
                    log.info("x%s : %s", str(var_id), self.get_site(index).get_data_manager().find_most_recent_snapshot(txn.get_start_time() ,var_id, txn.get_id()))
                    txn.set_status(TransactionStatus.RUNNING)


            self.waiting_txn_even_var[index][var_id] = []
            cleared_variables.append(var_id)

        log.debug("Clearing pending reads for these variables from other sites")
        for site in self.get_all_sites() :
            to_be_cleared_list = self.waiting_txn_even_var[site.get_id()]

            for index in cleared_variables :
                if index in to_be_cleared_list.keys():
                    log.debug("Clearing pending reads list on variable %s from Site %s", index, site.get_id())
                    self.waiting_txn_even_var[site.get_id()][index] = []

        # Odd Indexed Variables
        if index in self.waiting_txn.keys():
            wait_txn_list = self.waiting_txn[index]
            log.info("Executing Pending reads for odd indexed variables as site recovered")
            for record in wait_txn_list:
                # Site is UP
                txn = record[0]
                var_index = record[1]
                if txn.get_status() == TransactionStatus.WAITING and not(txn.get_status() == TransactionStatus.ABORTED) :
                    log.info("Txn %s : Reading  x%s ", txn.get_name(), str(var_index))
                    log.info("x%s : %s", str(var_index), self.get_site(index).get_data_manager().find_most_recent_snapshot(txn.get_start_time() ,var_index, txn.get_id()))
                    txn.set_status(TransactionStatus.RUNNING)

    def get_site_failure_history(self, index):
        """
        Returns the site_failure_history of that particular site identifued by index

        Parameters:
            index: Index of the site
        """
        return self.site_failure_history[index]

    def get_site_recover_history(self, index):
        """
        Returns the site_recover_history of that particular site identifued by index

        Parameters:
            index: Index of the site
        """
        return self.site_recover_history[index]
