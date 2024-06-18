"""
Authors:
1) Joel Marvin Tellis (jt4680)
2) Sahil Bakshi (sb8916)
"""
import logging
from RepCRec.Instruction import Instruction
# from Variable import Variable
from RepCRec.constants import SITE_MANAGER_FUNCS

log = logging.getLogger(__name__)

class Simulator:
    """
    Simulator for the project. Runs a discreate event simulator

    Parameters:
        file_name : input file to read instrcutions from
        site_manager : Instance of Site Manager
        transaction_manager : Instance of Transaction Manager
    """
    def __init__(self, file_name, site_manager, transaction_manager):

        self.file_name = file_name
        self.line_generator = self._get_line_generator()
        self.site_manager = site_manager
        self.transaction_manager = transaction_manager
        self.current_time = 0

    def get_next_instruction(self):
        """
        Fetchs the next line and processes the instruction on that line

        Returns:
            Instance of Instruction Class with TYPE and PARAMS attributes
        """
        line = next(self.line_generator, None)

        if line is None:
            return line
        else:
            # log.info(" ------- Processing Line : %s", line)
            return self._process_instruction(line)

    def _get_line_generator(self):
        """
        Fetch the next instruction from the input file

        Returns:
            Line from the input file
        """
        with open(self.file_name, 'r', encoding='UTF-8') as input_file:
            for line in input_file:
                if len(line) > 1:
                    yield line

    def _process_instruction(self, line):
        """
        Processes the line from the input file
        """
        line = line.strip().split(";")
        instructions = []
        for instruction in line:
            if instruction.find("//") == 0:
                # Ignore comments
                continue
            instructions.append(Instruction(instruction))

        return instructions

    def run(self):
        """
        Run the Discrete Event Simulator
        """
        instructions = self.get_next_instruction()
        self.current_time += 1

        while instructions is not None:
            for instruction in instructions:
                # Increment global time
                self.current_time += 1

                if instruction.get_instruction_type() in SITE_MANAGER_FUNCS:
                    self.site_manager.process_instr(self.current_time, instruction)
                else:
                    self.transaction_manager.process_instr(self.current_time, instruction)

            instructions = self.get_next_instruction()
