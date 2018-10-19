
# Global Constants:

SIZE_OPTIONS = ["micro", "small", "medium", "large", "super large"]
CLASS_OPTIONS = ["open", "commercial", "customer", "customer_financial", "customer_health", "customer_personal"]

NAMINGCONV = {"truth_prefix": "dp_tta",
              "short_prefix": "tta",
              "sql_root": "sql/dp/"
              }

JOB_ENVIRONS_orig= "\"dev\",\"ppd\""
JOB_ENVIRONS= r'["dev","ppd"]'
JDBCPORTNUMBER = "1433"

JOBS_PATH = "jobs/"
SQL_PATH = "sql/"
CONFIGS_PATH = "configs/"
COLUMNS_PATH = "columns/"
TEMPLATES_PATH = "templates/"
WRITE_OUTPUT_FILE = "output/jobs/"
WRITE_SQL_FILE = "output/sql/"
DEFAULT_START_TIME = "2000-01-01 00:00:00"
DEFAULT_INTERVAL = ""

