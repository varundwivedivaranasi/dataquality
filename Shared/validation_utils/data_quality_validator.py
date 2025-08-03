from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, trim, current_timestamp
from typing import List, Dict
import uuid

class DataQualityValidator:
    def __init__(self, df: DataFrame, notebook_name: str):
        self.df = df
        self.notebook_name = notebook_name
        self.timestamp = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]
        self.results = []

    def check_not_null_or_blank(self, rule: Dict):
        failed_df = self.df.filter(
            col(rule["column"]).isNull() | (trim(col(rule["column"])) == "")
        ).withColumn("error", lit(rule["description"])) \
         .withColumn("rule_id", lit(rule["id"])) \
         .withColumn("category", lit(rule["category"])) \
         .withColumn("severity", lit(rule["severity"])) \
         .withColumn("notebook_name", lit(self.notebook_name)) \
         .withColumn("timestamp", lit(str(self.timestamp)))

        self.results.append({
            "rule_id": rule["id"],
            "rule": rule["description"],
            "category": rule["category"],
            "severity": rule["severity"],
            "failed_count": failed_df.count(),
            "failed_records": failed_df
        })

    def check_pattern(self, rule: Dict):
        failed_df = self.df.filter(~col(rule["column"]).rlike(rule["pattern"])) \
            .withColumn("error", lit(rule["description"])) \
            .withColumn("rule_id", lit(rule["id"])) \
            .withColumn("category", lit(rule["category"])) \
            .withColumn("severity", lit(rule["severity"])) \
            .withColumn("notebook_name", lit(self.notebook_name)) \
            .withColumn("timestamp", lit(str(self.timestamp)))

        self.results.append({
            "rule_id": rule["id"],
            "rule": rule["description"],
            "category": rule["category"],
            "severity": rule["severity"],
            "failed_count": failed_df.count(),
            "failed_records": failed_df
        })

    def check_custom_sql(self, rule: Dict):
        expression = rule.get("expression")
        failed_df = self.df.filter(expr(expression)) \
            .withColumn("error", lit(rule.get("description", "Custom SQL check failed"))) \
            .withColumn("rule_id", lit(rule.get("id", "NA"))) \
            .withColumn("category", lit(rule.get("category", "unspecified"))) \
            .withColumn("severity", lit(rule.get("severity", "warning"))) \
            .withColumn("notebook_name", lit(self.notebook_name)) \
            .withColumn("timestamp", lit(str(self.timestamp)))

        self.results.append({
            "rule_id": rule.get("id", "NA"),
            "rule": rule.get("description", "Custom SQL check failed"),
            "category": rule.get("category", "unspecified"),
            "severity": rule.get("severity", "warning"),
            "failed_count": failed_df.count(),
            "failed_records": failed_df
    })

    def run_checks(self, rules: List[Dict]):
        for rule in rules:
            if rule["type"] == "not_null_or_blank":
                self.check_not_null_or_blank(rule)
            if rule["type"] == "custom_sql":
                self.check_custom_sql(rule)
            elif rule["type"] == "pattern":
                self.check_pattern(rule)

    def show_results(self):
        for res in self.results:
            print(f"[{res['severity'].upper()}] Rule: {res['rule']} | Failed Count: {res['failed_count']}")
            res['failed_records'].show(truncate=False)

    def get_summary_df(self) -> DataFrame:
        summary_data = [(r["rule_id"], r["rule"], r["category"], r["severity"],
                         r["failed_count"], self.notebook_name, str(self.timestamp)) for r in self.results]

        return spark.createDataFrame(summary_data, schema=[
            "rule_id", "rule", "category", "severity", "failed_count", "notebook_name", "timestamp"
        ])

    def log_errors_to_blob(self, base_path: str):
        # Format timestamp for folder naming
        now = datetime.datetime.now()
        timestamp_str = now.strftime("%Y-%m-%dT%H-%M-%S")
        reject_path = f"{base_path}/rejects_{timestamp_str}"

        #write the summary into reject
        summary_df = validator.get_summary_df()
        summary_df.write.mode("overwrite").option("header", "true").csv(f"{reject_path}/summary")
        #write the data into reject
        for res in self.results:
            if res["failed_records"].count() > 0:
                res["failed_records"].write.mode("append").json(reject_path)
