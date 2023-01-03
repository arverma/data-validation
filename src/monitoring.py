# Why set spark_version: [Doc] https://pypi.org/project/pydeequ/
import os

os.environ["SPARK_VERSION"] = "3.0"
print(os.getenv("SPARK_VERSION"))

import pydeequ
from pydeequ.analyzers import (
    Size,
    AnalysisRunner,
    ApproxQuantiles,
    Mean,
    Maximum,
    Minimum,
    StandardDeviation,
    Completeness,
    CountDistinct,
    AnalyzerContext,
)

num_dtypes = [
    "IntegerType",
    "FloatType",
    "DoubleType",
    "LongType",
    "ByteType",
    "ShortType",
    "DecimalType",
]
cat_dtypes = ["StringType", "VarcharType", "CharType"]
bin_dtypes = ["BooleanType", "BinaryType"]


def run_feature_monitoring_analysis(spark, df):
    """

    :param spark:
    :param df:
    :param config:
    :return:
    """
    spark.sparkContext._conf.setAll(
        [
            ("spark.jars.packages", pydeequ.deequ_maven_coord),
            ("spark.jars.excludes", pydeequ.f2j_maven_coord),
        ]
    )
    analysis_runner = AnalysisRunner(spark).onData(df).addAnalyzer(Size())

    col_data_type_dict = {}
    for column in df.schema:
        col_type = str(column.dataType).replace("()", "")
        if col_type in num_dtypes:
            col_data_type_dict["Numeric"] = col_data_type_dict.get("Numeric", []) + [
                column.name
            ]
        elif col_type in cat_dtypes:
            col_data_type_dict["Categorical"] = col_data_type_dict.get(
                "Categorical", []
            ) + [column.name]
        elif col_type in bin_dtypes:
            col_data_type_dict["Binary"] = col_data_type_dict.get("Binary", []) + [
                column.name
            ]

    for col in col_data_type_dict.get("Numeric", []):
        analysis_runner.addAnalyzer(ApproxQuantiles(col, [0.25, 0.5, 0.75, 0.9]))
        analysis_runner.addAnalyzer(Mean(col))
        analysis_runner.addAnalyzer(Maximum(col))
        analysis_runner.addAnalyzer(Minimum(col))
        analysis_runner.addAnalyzer(StandardDeviation(col))
        analysis_runner.addAnalyzer(Completeness(col))

    for col in col_data_type_dict.get("Categorical", []):
        analysis_runner.addAnalyzer(CountDistinct(col))
        analysis_runner.addAnalyzer(Completeness(col))

    for col in col_data_type_dict.get("Binary", []):
        analysis_runner.addAnalyzer(CountDistinct(col))
        analysis_runner.addAnalyzer(Completeness(col))

    analysis_result = analysis_runner.run()

    return (
        AnalyzerContext.successMetricsAsDataFrame(spark, analysis_result)
        .withColumnRenamed("instance", "feature_name")
        .withColumnRenamed("name", "metric_name")
    )
