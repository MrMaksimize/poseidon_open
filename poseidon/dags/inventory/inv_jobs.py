"""inventory_jobs file."""
from poseidon.util import general
import pandas as pd
import logging


conf = general.config


def inventory_to_csv():
    inventory_prod_path = conf['prod_data_dir'] + '/inventory_datasd.csv'
    df = pd.read_csv("https://docs.google.com/spreadsheets/d/1LAx0GyM-HNbqsKg5zp-sKB2ieQ5nQQChWgazBuqy8d4/pub?gid=970785642&single=true&output=csv")
    general.pos_write_csv(df, inventory_prod_path)

    return "Successfuly wrote inventory file to prod."
