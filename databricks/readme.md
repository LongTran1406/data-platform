$env:DATABRICKS_HOST = "https://adb-<new-id>.<n>.azuredatabricks.net"
$env:DATABRICKS_TOKEN = "<your-token>"


databricks bundle deploy -t staging

databricks bundle run -t staging fetch_schedule_job