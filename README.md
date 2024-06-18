# RepCRec - Replicated Concurrency Control and Recovery project

Deisgned a **distributed database** system featuring 
- **Serializable Snapshot Isolation (SSI)** for consistency 
- **First-committer wins** rule for concurrency control 
- **Available copies** approach for fault tolerance and recovery.

## Authors:
1. Joel Marvin Tellis 

## Usage Instrcutions

### To execute the code
Execute `python3 -m RepCRec.start <PATH_TO_INPUT_FILE>` to see the code in action.

### User Manual
To get a user manual use the below command
`$ python3 -m RepCRec.start --help`

usage: start.py [-h] [-n 10] [-v 20] [-o None] file_path

positional arguments:
  file_path             Input file path.

options:
  -h, --help            show this help message and exit
  -n 10, --num-sites 10
                        Number of Sites
  -v 20, --num-variables 20
                        Number of variables
  -o None, --out-file None
                        Output file, if not passed output will be printed to std output (console)

NOTE: Even if Output file is specified, dump() will print Site information to the terminal only.

### Dependencies
Please use `python 3.10`. Following is the list of depencies for our project

- plac (for parsing command line arguments)

### To install dependencies
- Run `pip3 install plac` to install all of the requirements using pip3.

### Additional Notes
- Incase you wish to see a more detailed output indicating each action taken for each instruction,  you can change the level of Loggging in `config.py` file. Change the Value from `INFO` to `DEBUG`.