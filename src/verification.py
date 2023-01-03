# Why set spark_version: [Doc] https://pypi.org/project/pydeequ/
import os

os.environ["SPARK_VERSION"] = "3.0"
print(os.getenv("SPARK_VERSION"))

import pydeequ
from pydeequ.checks import CheckLevel
from pydeequ.verification import Check, VerificationSuite, VerificationResult

from src.verification_helper import construct_data_verification_checks


def run_feature_verification_analysis(spark, df, constraints):
    spark.sparkContext._conf.setAll(
        [
            ("spark.jars.packages", pydeequ.deequ_maven_coord),
            ("spark.jars.excludes", pydeequ.f2j_maven_coord),
        ]
    )

    check = Check(spark, CheckLevel.Error, "Review Check")

    checked_result = (
        VerificationSuite(spark)
        .onData(df)
        .addCheck(eval(construct_data_verification_checks(constraints)))
        .run()
    )

    return VerificationResult.checkResultsAsDataFrame(
        spark, checked_result
    ).withColumnRenamed("instance", "rule")
