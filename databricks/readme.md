$env:DATABRICKS_HOST = "https://adb-<new-id>.<n>.azuredatabricks.net"
$env:DATABRICKS_TOKEN = "<your-token>"


databricks bundle deploy -t staging

databricks bundle run -t staging pipeline_job

databricks bundle generate dashboard --existing-path "/Users/tranthelong1406@gmail.com/test.lvdash.json" --dashboard-dir dashboards -t staging