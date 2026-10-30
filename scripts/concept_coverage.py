"""Concept coverage audit: scans the project's code for each concept on the design checklist.
Run (locally, no Docker needed): python scripts/concept_coverage.py
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FOLDERS = ["src", "pipelines", "sandbox", "scripts"]
SKIP = {"concept_coverage.py"}  # this file contains every pattern, so it would match itself

# (area, concept, regular expression that proves the concept is used)
CHECKLIST = [
    ("Python OOP", "abstract base class (ABC)", r"\(ABC\)"),
    ("Python OOP", "@abstractmethod", r"@abstractmethod"),
    ("Python OOP", "inheritance from a Base class", r"^class \w+\(Base\w+\)"),
    ("Python OOP", "polymorphism (step.transform on any step)", r"step\.transform\("),
    ("Python OOP", "composition (Pipeline holds a list of steps)", r"self\._steps"),
    ("Python OOP", "encapsulation (protected attributes)", r"self\._\w+ ="),
    ("Python OOP", "@property", r"^\s*@property"),
    ("Python OOP", "property setter", r"\.setter"),
    ("Python OOP", "@dataclass", r"@dataclass"),
    ("Python OOP", "@staticmethod", r"@staticmethod"),
    ("Python OOP", "@classmethod", r"@classmethod"),
    ("Python OOP", "custom exception classes", r"class \w+\((PipelineError|Exception)\)"),
    ("Python OOP", "context manager (__enter__)", r"def __enter__"),
    ("Python OOP", "context manager (@contextmanager)", r"@contextlib\.contextmanager"),
    ("Python OOP", "decorator (functools.wraps)", r"functools\.wraps"),
    ("Python OOP", "decorator with arguments (retry)", r"def retry\("),
    ("Python OOP", "decorator applied to real code", r"^\s*@(log_execution_time|retry)\b"),
    ("Python OOP", "__repr__", r"def __repr__"),
    ("Python OOP", "__len__", r"def __len__"),
    ("Python OOP", "type hints", r"\) -> \w"),
    ("Python OOP", "list comprehension", r"\[[^\]\n]+ for \w+ in "),
    ("Python OOP", "dict comprehension", r"\{[^}\n]+ for [\w, ]+ in "),
    ("Python OOP", "try / finally cleanup", r"^\s*finally:"),
    ("Python OOP", "command-line arguments (argparse)", r"argparse\.ArgumentParser"),

    ("PySpark: session and IO", "SparkSession builder", r"SparkSession\.builder"),
    ("PySpark: session and IO", "runtime conf set/get", r"\.conf\.(set|get)\("),
    ("PySpark: session and IO", "read CSV", r"\.csv\("),
    ("PySpark: session and IO", "read JSON", r"\.json\("),
    ("PySpark: session and IO", "read JDBC", r"format\(\"jdbc\"\)|\.jdbc\("),
    ("PySpark: session and IO", "explicit StructType schema", r"StructType\("),
    ("PySpark: session and IO", "read mode PERMISSIVE", r"PERMISSIVE"),
    ("PySpark: session and IO", "read mode DROPMALFORMED", r"DROPMALFORMED"),
    ("PySpark: session and IO", "read mode FAILFAST", r"FAILFAST"),
    ("PySpark: session and IO", "corrupt-record column", r"_corrupt_record"),
    ("PySpark: session and IO", "inferSchema", r"inferSchema"),
    ("PySpark: session and IO", "write mode overwrite", r"\.mode\(\"overwrite\"\)"),
    ("PySpark: session and IO", "write mode append", r"\.append\(\)|\.mode\(\"append\"\)"),
    ("PySpark: session and IO", "Hive-style partitionBy write", r"\.write\.mode\(\"overwrite\"\)\.partitionBy\("),
    ("PySpark: session and IO", "bucketBy", r"\.bucketBy\("),

    ("PySpark: transformations", "select", r"\.select\("),
    ("PySpark: transformations", "selectExpr", r"\.selectExpr\("),
    ("PySpark: transformations", "withColumn", r"\.withColumn\("),
    ("PySpark: transformations", "withColumnRenamed", r"\.withColumnRenamed\("),
    ("PySpark: transformations", "drop", r"\.drop\("),
    ("PySpark: transformations", "filter", r"\.filter\("),
    ("PySpark: transformations", "isin", r"\.isin\("),
    ("PySpark: transformations", "like / ilike", r"\.i?like\("),
    ("PySpark: transformations", "rlike", r"\.rlike\("),
    ("PySpark: transformations", "na.fill / na.drop / dropna", r"\.na\.(fill|drop|replace)|\.dropna\("),
    ("PySpark: transformations", "isNull / isNotNull", r"\.isNull\(\)|\.isNotNull\(\)"),
    ("PySpark: transformations", "coalesce() column function", r"F\.coalesce\("),
    ("PySpark: transformations", "cast", r"\.cast\("),
    ("PySpark: transformations", "to_date", r"F\.to_date\("),
    ("PySpark: transformations", "to_timestamp", r"F\.to_timestamp\("),
    ("PySpark: transformations", "date_format", r"F\.date_format\("),
    ("PySpark: transformations", "datediff", r"F\.datediff\("),
    ("PySpark: transformations", "when", r"F\.when\("),
    ("PySpark: transformations", "otherwise", r"\.otherwise\("),
    ("PySpark: transformations", "regexp_replace", r"F\.regexp_replace\("),
    ("PySpark: transformations", "regexp_extract", r"F\.regexp_extract\("),
    ("PySpark: transformations", "split", r"F\.split\("),
    ("PySpark: transformations", "trim", r"F\.trim\("),
    ("PySpark: transformations", "concat_ws", r"F\.concat_ws\("),
    ("PySpark: transformations", "substring", r"F\.substring\("),
    ("PySpark: transformations", "dropDuplicates", r"\.dropDuplicates\("),
    ("PySpark: transformations", "distinct", r"\.distinct\("),
    ("PySpark: transformations", "orderBy", r"\.orderBy\("),
    ("PySpark: transformations", "union", r"\.union\("),
    ("PySpark: transformations", "unionByName", r"\.unionByName\("),

    ("PySpark: joins and nested data", "left_semi join", r"left_semi"),
    ("PySpark: joins and nested data", "left_anti join", r"left_anti"),
    ("PySpark: joins and nested data", "outer join", r"\"outer\""),
    ("PySpark: joins and nested data", "broadcast join hint", r"F\.broadcast\("),
    ("PySpark: joins and nested data", "explode", r"F\.explode\("),
    ("PySpark: joins and nested data", "array", r"F\.array\("),
    ("PySpark: joins and nested data", "array_contains", r"F\.array_contains\("),
    ("PySpark: joins and nested data", "struct", r"F\.struct\("),
    ("PySpark: joins and nested data", "collect_list", r"F\.collect_list\("),
    ("PySpark: joins and nested data", "nested field access", r"F\.col\(\"item\.\w+\"\)"),
    ("PySpark: joins and nested data", "scalar UDF", r"@F\.udf\(|F\.udf\("),
    ("PySpark: joins and nested data", "pandas_udf", r"pandas_udf"),

    ("PySpark: aggregation and windows", "groupBy", r"\.groupBy\("),
    ("PySpark: aggregation and windows", "agg", r"\.agg\("),
    ("PySpark: aggregation and windows", "countDistinct", r"F\.countDistinct\("),
    ("PySpark: aggregation and windows", "pivot", r"\.pivot\("),
    ("PySpark: aggregation and windows", "rollup", r"\.rollup\("),
    ("PySpark: aggregation and windows", "cube", r"\.cube\("),
    ("PySpark: aggregation and windows", "grouping()", r"F\.grouping\("),
    ("PySpark: aggregation and windows", "Window.partitionBy", r"Window\.partitionBy\("),
    ("PySpark: aggregation and windows", "row_number", r"F\.row_number\("),
    ("PySpark: aggregation and windows", "rank", r"F\.rank\("),
    ("PySpark: aggregation and windows", "dense_rank", r"F\.dense_rank\("),
    ("PySpark: aggregation and windows", "lag", r"F\.lag\("),
    ("PySpark: aggregation and windows", "lead", r"F\.lead\("),
    ("PySpark: aggregation and windows", "rowsBetween", r"\.rowsBetween\("),

    ("PySpark: performance and low level", "cache", r"\.cache\(\)"),
    ("PySpark: performance and low level", "persist with a storage level", r"\.persist\("),
    ("PySpark: performance and low level", "unpersist", r"\.unpersist\("),
    ("PySpark: performance and low level", "repartition", r"\.repartition\("),
    ("PySpark: performance and low level", "coalesce(n) partitions", r"\.coalesce\(\d+\)"),
    ("PySpark: performance and low level", "explain", r"\.explain\("),
    ("PySpark: performance and low level", "broadcast variable", r"sc\.broadcast\("),
    ("PySpark: performance and low level", "accumulator", r"sc\.accumulator\("),
    ("PySpark: performance and low level", "RDD from a DataFrame", r"\.rdd\b"),
    ("PySpark: performance and low level", "RDD flatMap", r"\.flatMap\("),
    ("PySpark: performance and low level", "RDD reduceByKey", r"\.reduceByKey\("),

    ("PySpark: actions and sampling", "show", r"\.show\("),
    ("PySpark: actions and sampling", "collect", r"\.collect\(\)"),
    ("PySpark: actions and sampling", "take / head", r"\.(take|head)\("),
    ("PySpark: actions and sampling", "foreach", r"\.foreach\("),
    ("PySpark: actions and sampling", "describe", r"\.describe\("),
    ("PySpark: actions and sampling", "summary", r"\.summary\("),
    ("PySpark: actions and sampling", "sample", r"\.sample\("),
    ("PySpark: actions and sampling", "randomSplit", r"\.randomSplit\("),
    ("PySpark: actions and sampling", "toPandas", r"\.toPandas\("),

    ("Iceberg: tables and partitioning", "namespaces", r"CREATE NAMESPACE"),
    ("Iceberg: tables and partitioning", "writeTo().createOrReplace()", r"createOrReplace\(\)"),
    ("Iceberg: tables and partitioning", "format version 2", r"format-version"),
    ("Iceberg: tables and partitioning", "days() transform", r"F\.days\("),
    ("Iceberg: tables and partitioning", "months() transform", r"F\.months\("),
    ("Iceberg: tables and partitioning", "bucket() transform", r"F\.bucket\("),
    ("Iceberg: tables and partitioning", "truncate() transform", r"truncate\("),
    ("Iceberg: tables and partitioning", "MERGE INTO", r"MERGE INTO"),

    ("Iceberg: evolution and time travel", "ADD COLUMN", r"ADD COLUMN"),
    ("Iceberg: evolution and time travel", "RENAME COLUMN", r"RENAME COLUMN"),
    ("Iceberg: evolution and time travel", "widen a column type", r"ALTER COLUMN \w+ TYPE"),
    ("Iceberg: evolution and time travel", "DROP COLUMN", r"DROP COLUMN"),
    ("Iceberg: evolution and time travel", "partition evolution", r"REPLACE PARTITION FIELD"),
    ("Iceberg: evolution and time travel", "VERSION AS OF", r"VERSION AS OF"),
    ("Iceberg: evolution and time travel", "TIMESTAMP AS OF", r"TIMESTAMP AS OF"),
    ("Iceberg: evolution and time travel", "snapshot listing", r"\.snapshots\(\)"),

    ("Iceberg: row-level changes and maintenance", "DELETE FROM", r"DELETE FROM"),
    ("Iceberg: row-level changes and maintenance", "UPDATE ... SET", r"UPDATE .* SET"),
    ("Iceberg: row-level changes and maintenance", "merge-on-read", r"merge-on-read"),
    ("Iceberg: row-level changes and maintenance", "rewrite_data_files (compaction)", r"rewrite_data_files"),
    ("Iceberg: row-level changes and maintenance", "expire_snapshots", r"expire_snapshots"),
    ("Iceberg: row-level changes and maintenance", "remove_orphan_files", r"remove_orphan_files"),
    ("Iceberg: row-level changes and maintenance", "rewrite_manifests", r"rewrite_manifests"),
    ("Iceberg: row-level changes and maintenance", "sort order", r"WRITE ORDERED BY"),

    ("Iceberg: branches, increments, engines", "branch", r"CREATE OR REPLACE BRANCH"),
    ("Iceberg: branches, increments, engines", "tag", r"CREATE OR REPLACE TAG"),
    ("Iceberg: branches, increments, engines", "write-audit-publish property", r"write\.wap\.enabled"),
    ("Iceberg: branches, increments, engines", "fast_forward", r"fast_forward"),
    ("Iceberg: branches, increments, engines", "incremental read", r"start-snapshot-id"),
    ("Iceberg: branches, increments, engines", "changelog view", r"create_changelog_view"),
    ("Iceberg: branches, increments, engines", "second engine (DuckDB)", r"iceberg_scan"),
]


def load_sources() -> dict:
    sources = {}
    for folder in FOLDERS:
        for path in sorted((ROOT / folder).rglob("*.py")):
            if path.name not in SKIP:
                sources[str(path.relative_to(ROOT))] = path.read_text(encoding="utf-8")
    return sources


def main() -> None:
    sources = load_sources()
    print(f"Scanning {len(sources)} Python files in {', '.join(FOLDERS)}")
    missing = []
    current_area = None
    for area, concept, pattern in CHECKLIST:
        if area != current_area:
            print(f"\n{area}")
            current_area = area
        regex = re.compile(pattern, re.MULTILINE)
        hits = [name for name, text in sources.items() if regex.search(text)]
        if hits:
            print(f"  [x] {concept:<44s} {len(hits):>2} file(s), e.g. {hits[0]}")
        else:
            print(f"  [ ] {concept:<44s} NOT FOUND")
            missing.append(f"{area}: {concept}")
    print(f"\n{len(CHECKLIST) - len(missing)} of {len(CHECKLIST)} concepts found.")
    if missing:
        print("Missing:")
        for item in missing:
            print("  -", item)


if __name__ == "__main__":
    main()