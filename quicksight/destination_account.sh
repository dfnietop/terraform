# export AWS_PROFILE=ccdev

read -p "Enter aws-account-id: " aws_account_id
read -p "Enter object-name-prefix: \n This name will be used to create all dashboard, dataset, and datasource objects: " object_name_prefix
read -p "Enter dataset_description.json path [dataset_description.json]: " dataset_description_path
dataset_description_path=${dataset_description_path:-dataset_description.json}
read -p "Enter template_description.json path [template_description.json]: " template_description_path
template_description_path=${template_description_path:-template_description.json}
read -p "Enter principal arn for viewer permissions: " principal_arn

import_modes=("DIRECT_QUERY" "SPICE" "Exit")
echo "Select a destination domain:"
select opt in "${import_modes[@]}"; do
    case $opt in
        "DIRECT_QUERY")
            echo "You selected DIRECT_QUERY"
            import_mode="DIRECT_QUERY"
            break
            ;;
        "SPICE")
            echo "You selected SPICE"
            import_mode="SPICE"
            break
            ;;
        "Exit")
            echo "Exiting..."
            exit 1
            ;;
        *) 
            echo "Invalid option. Please choose a valid number."
            ;;
    esac
done
cat << EOF
You have entered the following arguments:
* aws_account_id : $aws_account_id
* object-name-prefix : $object_name_prefix
* dataset_description_path : $dataset_description_path
* template_description_path : $template_description_path
* principal_arn : $principal_arn
This will create The following quicksight objects:
* Datasource = $object_name_prefix-dtsrc
* Dataset = $object_name_prefix-dtst
* Dashboard = $object_name_prefix-dsh
EOF
read -r -p "Do you want to continue? [y/N] " response
case "$response" in
    [yY][eE][sS]|[yY]) 
        echo "Starting Script"
        ;;
    *)
        echo "Exiting..."
        exit 1
        ;;
esac
#####################
# DATASOURCE CREATION
#####################
echo "STARTING DATA_SOURCE CREATION"
data_source_id=$(uuidgen)

if data_source_arn=$(aws quicksight create-data-source \
--aws-account-id $aws_account_id \
--data-source-id $data_source_id \
--name "$object_name_prefix-dtsrc" \
--type "ATHENA" \
--data-source-parameters AthenaParameters={WorkGroup=primary})
then
    data_source_arn=$(echo "$data_source_arn" | jq -r ".Arn")
    echo "Datasource Created Successfully"
    echo "Datasource ID: $data_source_id"
    echo "Datasource Arn: $data_source_arn"
else
    echo "Datasource Creation Failed"
    echo $data_source_arn
    exit 1
fi

#####################
# DATASET CREATION
#####################
echo "STARTING DATA_SET CREATION"


json_content=$(jq .DataSet.PhysicalTableMap $dataset_description_path)
# modified_json=$(echo "$json_content" | jq --arg dataasource "$data_source_arn" 'walk(if type == "object" and has("DataSourceArn") then .DataSourceArn = $dataasource else . end)')
# echo "$modified_json" > "physical-table-map.json"

modified_json=$(python -c '
import json,sys
datasource = sys.argv[1:][1]
with open(sys.argv[1:][0]) as file:
    json_data = json.load(file)
    
def update_data_source_arn(obj):
    if isinstance(obj, dict):
        if "DataSourceArn" in obj:
            obj["DataSourceArn"] = datasource
        for value in obj.values():
            update_data_source_arn(value)
    elif isinstance(obj, list):
        for item in obj:
            update_data_source_arn(item)

update_data_source_arn(json_data)

# Convert the modified JSON back to a string
modified_json_str = json.dumps(json_data["DataSet"]["PhysicalTableMap"], indent=2)
print(modified_json_str)
' "$dataset_description_path" "$data_source_arn")

echo "$modified_json" > "physical-table-map.json"

json_content=$(jq .DataSet.LogicalTableMap $dataset_description_path)
echo "$json_content" > "logical-table-map.json"

if [ "$json_content" = "null" ]
then
logicalmap_command=""
else
logicalmap_command="--logical-table-map file://logical-table-map.json"
fi


json_content=$(jq .DataSet.ColumnGroups $dataset_description_path)
echo "$json_content" > "column-groups.json"

if [ "$json_content" = "null" ]
then
column_command=""
else
column_command="--column-groups file://column-groups.json"
fi


json_content=$(cat <<EOF
[
    {
        "Principal": "$principal_arn",
        "Actions": [
            "quicksight:PassDataSet",
            "quicksight:DescribeIngestion",
            "quicksight:CreateIngestion",
            "quicksight:UpdateDataSet",
            "quicksight:DeleteDataSet",
            "quicksight:DescribeDataSet",
            "quicksight:CancelIngestion",
            "quicksight:DescribeDataSetPermissions",
            "quicksight:ListIngestions",
            "quicksight:UpdateDataSetPermissions"
        ]
    }
]
EOF
)
echo "$json_content" > "user-permissions.json"


data_set_id=$(uuidgen)
if data_set_arn=$(aws quicksight create-data-set \
--aws-account-id $aws_account_id \
--region us-east-1 \
--data-set-id $data_set_id \
--name "$object_name_prefix-dtst" \
--import-mode "$import_mode" \
--physical-table-map file://physical-table-map.json $logicalmap_command $column_command \
--permissions file://user-permissions.json)
then
    data_set_arn=$(echo "$data_set_arn" | jq -r ".Arn")
    echo "Dataset Created Successfully"
    echo "Dataset ID: $data_set_id"
    echo "Dataset Arn: $data_set_arn"
else
    echo "Dataset Creation Failed"
    echo $data_set_arn
    exit 1
fi

#####################
# DASHBOARD CREATION
#####################
echo "STARTING DASHBOARD CREATION"
template_data_set_placeholder=$(jq .Template.Version.DataSetConfigurations[0].Placeholder $template_description_path)
template_arn=$(jq .Template.Arn $template_description_path)

json_content=$(cat <<EOF
{
"SourceTemplate": {
"DataSetReferences": [
{
"DataSetPlaceholder": $template_data_set_placeholder,
"DataSetArn": "$data_set_arn"
}
],
"Arn": $template_arn
}
}
EOF
)
echo "$json_content" > "dashboard.json"

dashboard_id=$(uuidgen)
if dashboard_arn=$(aws quicksight create-dashboard \
--aws-account-id $aws_account_id \
--dashboard-id $dashboard_id \
--name "$object_name_prefix-dsh" \
--source-entity file://dashboard.json)
then
    dashboard_arn=$(echo "$dashboard_arn" | jq -r ".Arn")
    echo "Dashboard Created Successfully"
    echo "Dashboard ID: $dashboard_id"
    echo "Dashboard Arn: $dashboard_arn"
else
    echo "Dashboard Creation Failed"
    echo $dashboard_arn
    exit 1
fi

json_content=$(cat <<EOF
[
  {
    "Principal": "$principal_arn",
    "Actions": [
      "quicksight:DescribeDashboard",
      "quicksight:ListDashboardVersions",
      "quicksight:UpdateDashboardPermissions",
      "quicksight:QueryDashboard",
      "quicksight:UpdateDashboard",
      "quicksight:DeleteDashboard",
      "quicksight:DescribeDashboardPermissions",
      "quicksight:UpdateDashboardPublishedVersion"
    ]
  }
]
EOF
)
echo "$json_content" > "dashboard-permissions.json"

if dashboard_permissions=$(aws quicksight update-dashboard-permissions \
--aws-account-id $aws_account_id \
--dashboard-id $dashboard_id \
--grant-permissions file://dashboard-permissions.json)
then
    echo "Dashboard permissions created successfully"
    echo "$dashboard_permissions"
else
    echo "$dashboard_permissions"
    exit 1
fi
echo "CLEANUP"
rm dashboard-permissions.json dashboard.json physical-table-map.json logical-table-map.json column-groups.json user-permissions.json