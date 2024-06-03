import os
import sys 
from importlib import reload

config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.append(config_path)

import connection 

etl_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'etl'))
sys.path.append(etl_path)
import extraction as de
# print(sys.path)



def main():
    pass
    # Reload connection module
    # connection = reload(connection)

    # Run ETL process
    de.extract_transform_load()

if __name__ == "__main__":
    main()

