
# Global Constants:
DEFAULT_INDEXING = ["id", "contract", "contractaccountnumber", "businesspartner", "type", "bptype"]
SIZE_OPTIONS = ["micro", "small", "medium", "large", "super large"]
CLASS_OPTIONS = ["open", "commercial", "customer", "customer_financial", "customer_health", "customer_personal"]
NAMINGCONV = {"truth_prefix": "dp_tta_",
              "short_prefix": "tta_",
              "sql_root": "sql/dp/",
              "parquet_load_location": "cap/",
              "stored_proc_prefix": "dp_cr_tta_"}
JOB_ENVIRONS = "\"dev\",\"ppd\""
JDBCPORTNUMBER = "1433"
# Where the scriptWriter reads/writes from/to:
JOBS_PATH = "jobs/"
SQL_PATH = "sql/"
CONFIGS_PATH = "truthabout/configs/"
COLUMNS_PATH = "truthabout/columns/"
TEMPLATES_PATH = "truthabout/templates/"
WRITE_OUTPUT_FILE = "truthabout/output/jobs/"
DEFAULT_START_TIME = "2000-01-01 00:00:00"
DEFAULT_INTERVAL = "\"\""

COLUMN_DOCUMENTATION = True

#global CLASSIFICATION