"""
Authors:
1) Joel Marvin Tellis (jt4680)
2) Sahil Bakshi (sb8916)
"""
import logging, copy
from collections import defaultdict
from itertools import groupby

from RepCRec.Transaction import Transaction
from RepCRec.enums.SiteStatus import SiteStatus
from RepCRec.enums.TransactionStatus import TransactionStatus
from RepCRec.constants import BEGIN_FUNC, WRITE_FUNC, READ_FUNC, END_FUNC

log = logging.getLogger(__name__)

class TransactionManager:
    """
    Transaction manager class is reponsible for handling transaction level activites. It is also responsible for calling site manager when a transaction commits.

    Parameters:
        num_vars (int): Number of variables
        num_sites (int): Number of sites
        site_manager (class object): Global instance of SiteManager

    Attributes:
        current_time (int) : The global time at this point
        transaction_map (dict): Maps Transaction ID to Transaction class object
        transaction_access_history (dict of dict): Helps determine conflict edges as inputs are read.
                                    KEY of outer hashmap is Txn ID and VALUE is inner dict respectively.
                                    Inner dict : KEY is Variable ID and values is list of character containing characters ('R' or 'W')
                                    to check for Read-Write or Write-Write conflicts when another concurrent transaction
                                    modifies the same data afterwards
        serialization_graph (dict of list) : Graph to detect cycles when a transaction commits.
    """
    def __init__(self, num_vars, num_sites, site_manager):
        self.number_of_variables = num_vars
        self.number_of_sites = num_sites
        self.transaction_map = dict()
        self.site_manager = site_manager
        self.current_time = 0

        def temp_dict():
            return defaultdict(list)
        self.transaction_access_history = defaultdict(temp_dict)

        self.serialization_graph = defaultdict(list)
        self.V = 0

    def addEdge(self, u, v):
        """
        Adds an edge to serialization graph from Node u to Node v
        """
        self.V = max(self.V, u)
        self.V = max(self.V, v)
        # print("self.V --> "+ str(self.V))
        if v not in self.serialization_graph[u]:
            self.serialization_graph[u].append(v)

    def isCyclicUtil(self, v, visited, recStack):
        """
        Helper to Detect Cycle in serialization graph
        """
        # Mark current node as visited and
        # adds to recursion stack
        visited[v] = True
        recStack[v] = True

        # Recur for all neighbours
        # if any neighbour is visited and in
        # recStack then graph is cyclic
        for neighbour in self.serialization_graph[v]:
            if visited[neighbour] == False:
                if self.isCyclicUtil(neighbour, visited, recStack) == True:
                    return True
            elif recStack[neighbour] == True:
                return True

        # The node needs to be popped from
        # recursion stack before function ends
        recStack[v] = False
        return False

    def isCyclic(self):
        """
        Detect Cycle in serialization graph
        """
        # self.V = len(self.serialization_graph)
        if(self.V == 0):
            return False

        # print(self.serialization_graph)
        visited = [False] * (self.V + 1)
        # print(visited)
        recStack = [False] * (self.V + 1)
        for node in range(1,self.V+1):
            if visited[node] == False:
                if self.isCyclicUtil(node, visited, recStack) == True:
                    return True
        return False

    def process_instr(self, current_time, instruction):
        """
        Method responsible for calling other methods based on instruction type.

        Parameters:
            current_time : The global time at this point
            instruction : object of class Instruction, contains the current instruction attributes
        """
        self.current_time = current_time
        params = instruction.get_params()

        if instruction.get_instruction_type() == BEGIN_FUNC:
            # being()
            self.begin(params)
        elif instruction.get_instruction_type() == READ_FUNC:
            # read()
            self.read_req(params)
        elif instruction.get_instruction_type() == WRITE_FUNC:
            # write()
            self.write_req(params)
        elif instruction.get_instruction_type() == END_FUNC:
            # end()
            self.end_txn(params)
        else:
            log.info("Invalid Instruction in Transaction Manager")

    def begin(self, params):
        """
        Method to initialize a transaction and making a new instance of Transaction class.
        We also note a mapping of newly created Transaction class with index in transaction_map

        Parameters:
            params : list of parameters of the parsed instruction, containing instruction name
        """
        log.info("Starting %s", params[0])
        txn_name =  params[0]
        txn_index = int(txn_name[1:])
        self.transaction_map[txn_index] = Transaction(txn_index, params[0], self.current_time)
        return

    def read_req(self, params):
        """
        Method to handle read instruction

        Parameters:
            params : list of parameters of the parsed instruction, containing instruction name, variable name
        """
        txn_name =  params[0]
        txn_index = int(txn_name[1:])
        txn_obj =  self.transaction_map[txn_index]

        var_name = params[1]
        var_index = int(var_name[1:])

        if var_index % 2 == 0 :
            # Even indexed variable - Available at all sites
            sites_to_be_added_for_wait = []
            for site in self.site_manager.get_all_sites():
                if site.get_status() == SiteStatus.UP or site.get_status() == SiteStatus.RECOVERED :
                    # Check 1 : site s was up all the time between the time when xi was committed and T began.
                    log.debug("CHECK 1 for Reading %s from Site %s by Txn %s", var_name, site.get_id(), txn_name)
                    time_var_last_committed = site.get_data_manager().get_committed_variable_before_time(txn_obj.get_start_time(), var_index)

                    # Failure History for this site
                    failure_history = self.site_manager.get_site_failure_history(site.get_id())

                    flg_case_1 = False

                    for fail_time in failure_history :
                        if fail_time > time_var_last_committed and fail_time < txn_obj.get_start_time():
                            # Site failed between the time xi was committed and T began. T can abort
                            log.debug("Site %s failed btw time when %s was committed and %s began . Going to next site", site.get_id(), var_name, txn_name)
                            flg_case_1 = True
                            break

                    if flg_case_1:
                        continue

                    # At this Stage -> We are sure this site did not fail between the time xi was committed and the T began()

                    # Check 2 : A read from a transaction that begins after the recovery of site s for a replicated variable x will not be allowed at s until a committed write to x takes place on s.
                    log.debug("CHECK 2 for Reading %s from Site %s by Txn %s", var_name, site.get_id(), txn_name)

                    # Recovery History for this site
                    entire_recover_history = self.site_manager.get_site_recover_history(site.get_id())
                    if len(entire_recover_history) == 1:
                        # Site never failed yet(till this current_time) and hence never had to recover
                        log.debug("Site %s never failed till this time", site.get_id())
                        sites_to_be_added_for_wait = []
                        log.info("Txn %s : Reading  %s from Site %s", txn_name, var_name, site.get_id())
                        log.info("%s : %s", var_name, site.get_data_manager().find_most_recent_snapshot(txn_obj.get_start_time() ,var_index, txn_obj.get_id()))
                        # Note that T accessed var:R
                        self.transaction_access_history[txn_index][var_index].append("R")
                        # Note that T accessed this site
                        self.transaction_map[txn_index].add_sites_accessed(site.get_id(), "R", self.current_time)
                        return

                    # At this stage -> we have some recovery history for this site
                    # Remove the first float('inf') value from history
                    entire_recover_history = entire_recover_history[1:]

                    recover_history_before_T_began = []

                    for t in entire_recover_history:
                        if t < txn_obj.get_start_time() :
                            recover_history_before_T_began.append(t)
                        else:
                            break

                    # Now we have list of timestamps when the site failed before T began

                    # Now we check if there was a write committed to xi at this site after the last timestamp in recover_history_before_T_began
                    flg = True
                    if len(recover_history_before_T_began) == 0:
                        # site never failed before T began and hence never had to recover before T began
                        log.debug("Site %s never failed before %s began", site.get_id(), txn_name)
                        pass
                    else:
                        log.debug("Checking if Write was committed on %s at Site %s ....", var_name, site.get_id())
                        flg = site.get_data_manager().check_commit_btw_time_range(recover_history_before_T_began[-1], txn_obj.get_start_time(), var_index)

                    if flg:
                        # write was committed and hence T can read xi value from this site
                        log.debug("Write was committed on %s at Site %s between recovery and %s began", var_name, site.get_id(), txn_name)
                        sites_to_be_added_for_wait = []
                        log.info("Txn %s : Reading  %s from Site %s", txn_name, var_name, site.get_id())
                        log.info("%s : %s", var_name, site.get_data_manager().find_most_recent_snapshot(txn_obj.get_start_time() ,var_index, txn_obj.get_id()))
                        # Note that T accessed var:R
                        self.transaction_access_history[txn_index][var_index].append("R")
                        # Note that T accessed this site
                        self.transaction_map[txn_index].add_sites_accessed(site.get_id(), "R", self.current_time)
                        return
                    else:
                        # Look for another site
                        log.debug("Write was not committed on %s at Site %s between recovery and %s began. Looking for next site", var_name, site.get_id(), txn_name)
                        continue

                elif site.get_status() == SiteStatus.DOWN :
                    # Site is currently down. Check if we need to wait
                    log.debug("Site %s DOWN. Checking if Read op can be put into Pending ...", site.get_id())
                    # Check 1 : site s was up all the time between the time when xi was committed and T began.
                    log.debug("CHECK 1 for Reading %s from Site %s by Txn %s", var_name, site.get_id(), txn_name)
                    time_var_last_committed = site.get_data_manager().get_committed_variable_before_time(txn_obj.get_start_time(), var_index)

                    # Failure History for this site
                    failure_history = self.site_manager.get_site_failure_history(site.get_id())

                    flg_case_1 = False

                    for fail_time in failure_history :
                        if fail_time > time_var_last_committed and fail_time < txn_obj.get_start_time():
                            # Site failed between the time xi was committed and T began. T can abort
                            log.debug("Site %s failed btw time when %s was committed and %s began . Going to next site", site.get_id(), var_name, txn_name)
                            flg_case_1 = True
                            break

                    if flg_case_1:
                        continue

                    # At this Stage -> We are sure this site did not fail between the time xi was committed and the T began()

                    # Check 2 : A read from a transaction that begins after the recovery of site s for a replicated variable x will not be allowed at s until a committed write to x takes place on s.
                    log.debug("CHECK 2 for Reading %s from Site %s by Txn %s", var_name, site.get_id(), txn_name)

                    # Recovery History for this site
                    entire_recover_history = self.site_manager.get_site_recover_history(site.get_id())
                    if len(entire_recover_history) == 1:
                        # Site never failed yet(till this current_time) and hence never had to recover
                        # Add txn read to pending state for this site
                        log.info("Txn %s has to be added for Pending Reading on %s from Site %s", txn_name, var_name, site.get_id())
                        sites_to_be_added_for_wait.append(site.get_id())
                        # self.site_manager.add_wait_txn_even(site.get_id(), self.transaction_map[int(txn_name[1:])], var_index)
                        continue

                    # At this stage -> we have some recovery history for this site
                    # Remove the first float('inf') value from history
                    entire_recover_history = entire_recover_history[1:]

                    recover_history_before_T_began = []

                    for t in entire_recover_history:
                        if t < txn_obj.get_start_time() :
                            recover_history_before_T_began.append(t)
                        else:
                            break

                    # Now we have list of timestamps when the site failed before T began

                    # Now we check if there was a write committed to xi at this site after the last timestamp in recover_history_before_T_began
                    flg = True
                    if len(recover_history_before_T_began) == 0:
                        # site never failed before T began and hence never had to recover before T began
                        log.debug("Site %s never failed before %s began", site.get_id(), txn_name)
                        pass
                    else:
                        log.debug("Checking if Write was committed on %s at Site %s ....", var_name, site.get_id())
                        flg = site.get_data_manager().check_commit_btw_time_range(recover_history_before_T_began[-1], txn_obj.get_start_time(), var_index)

                    if flg:
                        # write was committed and hence T can read xi value from this site

                        # Add txn read to pending state for this site
                        log.info("Txn %s has to be added for Pending Reading on %s from Site %s. Not Added yet", txn_name, var_name, site.get_id())
                        sites_to_be_added_for_wait.append(site.get_id())
                        # self.site_manager.add_wait_txn_even(site.get_id(), self.transaction_map[int(txn_name[1:])], var_index)
                        continue
                    else:
                        # Look for another site
                        log.debug("Write was not committed on %s at Site %s between recovery and %s began. Looking for next site", var_name, site.get_id(), txn_name)
                        continue
            # No Site could service the READ
            # Check if there were any sites that could be added for Pending READs
            if len(sites_to_be_added_for_wait) > 0 :
                for site_id in sites_to_be_added_for_wait :
                    log.info("Adding Txn %s for Pending Reading on %s from Site %s ...", txn_name, var_name, site_id)
                    self.site_manager.add_wait_txn_even(site_id, self.transaction_map[int(txn_name[1:])], var_index)
                    txn_obj.set_status(TransactionStatus.WAITING)
            else:
                log.info("Txn %s : Reading  %s FAILED AS NO VALID SITE FOUND", txn_name, var_name)
                log.info("Txn %s : ABORTING as READ failed", txn_name)
                txn_obj.set_status(TransactionStatus.ABORTED)

        else:
            # Odd Indexed variable - Only available at one site
            target_site_index = 1 + var_index % 10
            target_site = self.site_manager.get_site(target_site_index)

            if target_site.get_status() == SiteStatus.UP:
                # Site is UP
                log.info("Txn %s : Reading  %s ", txn_name, var_name)
                log.info("%s : %s", var_name, target_site.get_data_manager().find_most_recent_snapshot(txn_obj.get_start_time() ,var_index, txn_obj.get_id()))
                # Note that T accessed var:R
                self.transaction_access_history[txn_index][var_index].append("R")
                # Note that T accessed this site
                self.transaction_map[txn_index].add_sites_accessed(target_site.get_id(), "R", self.current_time)
            elif target_site.get_status() == SiteStatus.RECOVERED :
                # Site has recovered from failure
                # since index varibale is odd, we can perform the read from RECOVERED site
                log.info("Txn %s : Reading %s from RECOVERED site as odd indexed variable", txn_name, var_name)
                log.info("%s : %s", var_name, target_site.get_data_manager().find_most_recent_snapshot(txn_obj.get_start_time() ,var_index, txn_obj.get_id()))
                # Note that T accessed var:R
                self.transaction_access_history[txn_index][var_index].append("R")
                # Note that T accessed this site
                self.transaction_map[txn_index].add_sites_accessed(target_site.get_id(), "R", self.current_time)
            else:
                # Site is DOWN
                log.info("Txn %s : Reading  %s FAILED AS SITE %s IS DOWN. Adding to Waiting_txns...", txn_name, var_name, target_site_index)
                self.site_manager.add_wait_txn(target_site_index, self.transaction_map[int(txn_name[1:])], var_index)
                txn_obj.set_status(TransactionStatus.WAITING)


    def write_req(self, params):
        """
        Method to handle write instruction

        Parameters:
            params : list of parameters of the parsed instruction, containing instruction name, variable name, new value
        """
        txn_name =  params[0]
        txn_index = int(txn_name[1:])

        var_name = params[1]
        var_index = int(var_name[1:])
        var_value = int(params[2])

        if var_index % 2 == 0 :
            # Even indexed variable - Available at all sites
            for site in self.site_manager.get_all_sites():
                if site.get_status() == SiteStatus.UP:
                    # Site is UP, update the local copy of the site
                    site.get_data_manager().update_local_copy(int(txn_name[1:]), var_index, var_value)
                    log.info("Txn %s : Write  %s , Value %s, Site : %s UP", txn_name, var_name, params[2], site.get_id())
                    # Note that T accessed var:W
                    self.transaction_access_history[txn_index][var_index].append("W")
                    # Note that T accessed this site
                    self.transaction_map[txn_index].add_sites_accessed(site.get_id(), "W", self.current_time)
                elif site.get_status() == SiteStatus.RECOVERED:
                    # Site was previously down but now has recovered. Can service Write
                    log.info("Txn %s : Write  %s , Value %s, Site : %s RECOVERED site can service WRITE...", txn_name, var_name, params[2], site.get_id())
                    site.get_data_manager().update_local_copy(int(txn_name[1:]), var_index, var_value)
                    # Note that T accessed var:W
                    self.transaction_access_history[txn_index][var_index].append("W")
                    # Note that T accessed this site
                    self.transaction_map[txn_index].add_sites_accessed(site.get_id(), "W", self.current_time)
                else:
                    # Site is Down
                    log.info("Txn %s : Write  %s , Value %s, Site : %s FAILED as site is down", txn_name, var_name, params[2], site.get_id())
                    continue
        else:
            # Odd Indexed variable - Only available at one site
            target_site_index = 1 + var_index % 10
            target_site = self.site_manager.get_site(target_site_index)

            if target_site.get_status() == SiteStatus.UP :
                # Site is UP
                target_site.get_data_manager().update_local_copy(int(txn_name[1:]), var_index, var_value)
                log.info("Txn %s : Write  %s , Value %s, Site : %s odd index variable", txn_name, var_name, params[2], target_site.get_id())
                # Note that T accessed var:W
                self.transaction_access_history[txn_index][var_index].append("W")
                # Note that T accessed this site
                self.transaction_map[txn_index].add_sites_accessed(target_site.get_id(), "W", self.current_time)
            elif target_site.get_status() == SiteStatus.RECOVERED:
                # Site was previously down but now has recovered. Can service Write
                log.info("Txn %s : Write  %s , Value %s, Site : %s RECOVERED site can service WRITE for odd index...", txn_name, var_name, params[2], target_site.get_id())
                target_site.get_data_manager().update_local_copy(int(txn_name[1:]), var_index, var_value)
                # Note that T accessed var:W
                self.transaction_access_history[txn_index][var_index].append("W")
                # Note that T accessed this site
                self.transaction_map[txn_index].add_sites_accessed(target_site.get_id(), "W", self.current_time)
            else:
                # Site is DOWN
                log.info("Txn %s : Write %s , Value %s FAILED as site %s is down", txn_name, var_name, params[2], target_site.get_id())

    def end_txn(self, params):
        """
        Method to commit/abort transaction

        Parameters:
            params : list of parameters of the parsed instruction, containing instruction name
        """
        log.info("Txn %s : END. Checking whether to COMMIT/ABORT.....", params[0])

        txn_name =  params[0]
        txn_index = int(txn_name[1:])

        txn_obj =  self.transaction_map[txn_index]
        txn_start_time = txn_obj.get_start_time()

        if(txn_obj.get_status() == TransactionStatus.WAITING) :
            # Txn is waiting on some read
            log.info("Txn %s : is wating on some read. has to be ABORTED", txn_name)
            txn_obj.set_status(TransactionStatus.ABORTED)
            return

        #### When an end(T) occurs, for each access of T, determine whether T should abort either:

        # Case 1: for available copies reasons (i.e. T wrote x on a site that later failed)

        for site_id, operation, timestamp in self.transaction_map[txn_index].get_sites_accessed():
            # IF the site was accessed for WRITE
            if operation == "W":
                # Failure History for this site
                failure_history = self.site_manager.get_site_failure_history(site_id)

                # Recovery History for this site
                recover_history = self.site_manager.get_site_recover_history(site_id)

                # check if site failed after performing write
                for fail_time in failure_history :
                    if fail_time > timestamp :
                        # Abort transaction
                        log.info("Txn %s : ABORTED due to site failure", txn_name)
                        return

        # Case 2: for Snapshot Isolation reasons (i.e. some other transaction T' modified x after T began, T wrote x before or after' committed and T' committed before the end(T) occurred)

        variables_accessed = self.transaction_access_history[txn_index]

        for variable_index in variables_accessed.keys():
            if "W" in variables_accessed[variable_index]:
                if variable_index % 2 == 0:
                    # Even indexed variable accessed
                    for site in self.site_manager.get_all_sites():
                        if site.get_data_manager().get_committed_variable_time(variable_index) > txn_start_time :
                                # Abort transaction
                                log.info("Txn %s : ABORTED due to SSI reason", txn_name)
                                return
                else:
                    # Odd indexed variable accessed
                    target_site_index = 1 + variable_index % 10
                    target_site = self.site_manager.get_site(target_site_index)

                    if target_site.get_data_manager().get_committed_variable_time(variable_index) > txn_start_time :
                        # Abort transaction
                        log.info("Txn %s : ABORTED due to SSI reason", txn_name)
                        return

        # Case 3: Cycle in Serialization graph i.e. because committing T would create a cycle in the serialization graph including two rw edges in a row

        # Make a copy of the existing serialization graph
        graph = copy.deepcopy(self.serialization_graph)

        log.debug("Txn %s : Adding edges to serialization graph", txn_name)
        ## Current txn is considered (T')
        # Add Edges of current Txn to the serialization graph
        for variable_index in variables_accessed.keys():

            operations = variables_accessed[variable_index]
            w_flg_T_dash = ("W" in operations)
            r_flg_T_dash = ("R" in operations)
            T_dash_commit_time = txn_obj.get_commit_time()

            for inner_txn_idx, txn_object in self.transaction_map.items():

                if txn_object.get_status() == TransactionStatus.COMMITTED :
                    # print("--------- Transaction"+str(inner_txn_idx)+ " was in COMMITTED")
                    # Means txn_object is in the serialization graph
                    # For each committed transaction, considered (T)
                    inner_txn_var_accessed = self.transaction_access_history[inner_txn_idx]
                    if variable_index in inner_txn_var_accessed:
                        w_flg_T = ("W" in inner_txn_var_accessed[variable_index])
                        r_flg_T = ("R" in inner_txn_var_accessed[variable_index])
                        T_commit_time = txn_object.get_commit_time()

                        # Case 1 Upon end(T'),
                        # add T --ww--> T' to the serialization graph if T commits before T' begins, and they both write to x.
                        if T_commit_time is not None and T_commit_time < txn_start_time and w_flg_T_dash and w_flg_T :
                            self.addEdge(inner_txn_idx, txn_index)
                            log.debug("Adding Edge (Case 1) T%s --> T%s ", inner_txn_idx, txn_index)

                        # Case 2 Upon end(T'),
                        # add T --wr-->T' to the serialization graph if T writes to x, commits before T' begins, and T' reads from x.
                        if w_flg_T and r_flg_T_dash and T_commit_time is not None and T_commit_time < txn_start_time :
                            self.addEdge(inner_txn_idx, txn_index)
                            log.debug("Adding Edge (Case 2) T%s --> T%s ", inner_txn_idx, txn_index)

                        # Case 3 Upon end(T'),
                        # add T --rw --> T' to the serialization graph if T reads from x, T' writes to x, and T begins before end(T').
                        if r_flg_T and w_flg_T_dash and txn_object.get_start_time() < self.current_time :
                            self.addEdge(inner_txn_idx, txn_index)
                            log.debug("Adding Edge (Case 3.1) T%s --> T%s ", inner_txn_idx, txn_index)

                        if r_flg_T_dash and w_flg_T and txn_obj.get_start_time() < txn_object.get_commit_time() :
                            self.addEdge(txn_index, inner_txn_idx,)
                            log.debug("Adding Edge (Case 3.2) T%s --> T%s ", txn_index, inner_txn_idx)

        log.debug("Txn %s : Checking for cycle in serialization graph", txn_name)
        # Check if Cycle is formed
        if self.isCyclic() == 1:
            # IF Cycle, then ABORT, revert the serialization graph with the copy made at the start
            # Abort transaction
            log.debug("Graph contains cycle !!!!! ")
            log.info("Txn %s : ABORTED due to cycle in serialization graph", txn_name)
            self.serialization_graph = graph
            return
        else:
            # ELSE leave the serialization graph as it is
            log.debug("Graph doesn't contain cycle")

        # print(self.serialization_graph)
        ### COMMIT the transaction
        log.debug("Txn %s : ALL Fine. Trying to COMMIT...", txn_name)
        # print(self.transaction_map[txn_index].get_sites_accessed())
        # site_accessed_list = list(zip(*self.transaction_map[txn_index].get_sites_accessed()))[0]

        # shorthand to solve problem
        res = {key: [v[0] for v in val] for key, val in groupby(sorted(self.transaction_map[txn_index].get_sites_accessed(), key=lambda ele: ele[1]), key=lambda ele: ele[1])}
        # print(res)
        # print(site_accessed_list)
        for site in self.site_manager.get_all_sites():
            if site.get_id() in res.get("W", []) and (site.get_status() == SiteStatus.UP or site.get_status() == SiteStatus.RECOVERED):
                # log.info("Txn %s : COMMITTING TO SITE %s", txn_name, site.get_id())
                site.get_data_manager().commit_txn(txn_index, self.current_time)
                if site.get_status() == SiteStatus.RECOVERED :
                    log.info("Txn %s :Changing RECOVERED status to UP for Site %s", txn_name, site.get_id())
                site.set_status(SiteStatus.UP)

        log.info("Txn %s : COMMITTED SUCCESSFULLY", txn_name)
        txn_obj.set_commit_time(self.current_time)
        txn_obj.set_status(TransactionStatus.COMMITTED)
        return
