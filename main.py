"""

"""

from pyspark.sql import SparkSession
from src.monitoring import run_feature_monitoring_analysis
from src.verification import run_feature_verification_analysis
from src.utils import parse_known_cmd_args, read_job_config

if __name__ == "__main__":
    spark = SparkSession.builder.appName('test_app').getOrCreate()
    config = read_job_config(parse_known_cmd_args())

    df = None

    df_stats = run_feature_monitoring_analysis(spark, df)
    df_verification_report = run_feature_verification_analysis(spark, df, config["constraints"])

    # Why to stop spark session:
    # https://github.com/awslabs/python-deequ/issues/7#issuecomment-758169457
    spark.sparkContext._gateway.close()
    spark.stop()
