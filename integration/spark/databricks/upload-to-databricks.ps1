# Note: You need the Databricks CLI installed, and you need a token configured.
#       You may need to add a "--profile" option to each command if you want to
#       upload to a Databricks workspace that is not configured as your default
#       profile for the Databricks CLI.
#

Write-Output "Creating DBFS direcrtory"
$DBFS_OPENLINEAGE_DIR = "openlineage"
dbfs mkdirs dbfs:/databricks/$DBFS_OPENLINEAGE_DIR

Write-Output "Uploading custom Spark Listener library"
# Create a for loop here
$BUILD_PATH = "./build/libs/"
Get-ChildItem -Path $BUILD_PATH |
ForEach-Object {
    dbfs cp --overwrite $_.FullName     dbfs:/databricks/$DBFS_OPENLINEAGE_DIR/
}

Write-Output "Uploading cluster init script"
dbfs cp --overwrite ./databricks/open-lineage-init-script.sh           dbfs:/databricks/$DBFS_OPENLINEAGE_DIR/open-lineage-init-script.sh

Write-Output "Listing DBFS directory"
dbfs ls dbfs:/databricks/$DBFS_OPENLINEAGE_DIR
