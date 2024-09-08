# Import your utility classes
from utilities.sftp_utils import *
from utilities.db_utils import *
from utilities.rabbitmq_utils import *
from utilities.misc_utils import *
from utilities.customized_logger import DailyRotatingFileHandler
from utilities.logging_config import *
from charting.intraday_beta_v2 import *
from heatmaps_simulation.heatmap_task import *


prod = PostGreData(
    host=POSGRE_DB_HOST,
    port=POSGRE_DB_PORT,  # Default PostgreSQL port
    user=POSGRE_DB_USER,
    password=POSGRE_DB_PASSWORD,
    database=POSGRE_DB_NAME
)
logger.info(f'Postgre Status -- > {prod.get_status()}')

mysql = DatabaseUtilities(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, logger=logger)
logger.info(f'MySQL Status -- > {mysql.get_status()}')
dates = ["2024-08-23","2024-08-24","2024-08-25"]

for date in dates:
    data_read = mysql.execute_query(f"""SELECT * FROM optionsdepth_stage.charts_gamma where effective_date = '{date}'""")
    try:
        logger.info(f'Postgre Status -- > {prod.get_status()}')
        breakpoint()
        prod.insert_progress("public", "charts_gamma", data_read)
    except:
        print("Oh Oh")
        breakpoint()
        pass

breakpoint()