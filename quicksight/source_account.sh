aws_account_id=415231494060
aws_region="us-east-1"
read -p "Enter dashboard_name: " dashboard_name

domains=("Ciencuadras" "Jelpit Facilities" "Bolivar conmigo" "Exit")

# Display menu
echo "Select a destination domain:"
select opt in "${domains[@]}"; do
    case $opt in
        "Ciencuadras")
            echo "You selected Ciencuadras"
            domain="Ciencuadras"
            DEV=565999949569
            STG=505545527670
            PROD=290296201161
            break
            ;;
        "Jelpit Facilities")
            echo "You selected Jelpit Facilities"
            domain="Jelpit Facilities"
            DEV=460691020976
            STG=177040083317
            PROD=497030010780
            break
            ;;
        "Bolivar conmigo")
            echo "You selected Bolivar conmigo"
            domain="Bolivar conmigo"
            DEV=300342771130
            STG=989522218239
            PROD=390198534324
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
The template will be created with the following arguments:
* aws_account_id : $aws_account_id
* aws_region : $aws_region
* dashboard_name : $dashboard_name
And shared with the following accounts of $domain:
* DEV = $DEV
* STG = $STG
* PROD = $PROD
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


# Get Quicksight Analysis and Dataset ARN and ID
if dashboardid=$(aws quicksight list-dashboards \
--aws-account-id $aws_account_id \
--region $aws_region \
--query 'reverse(sort_by(DashboardSummaryList,&LastUpdatedTime))[?Name==`'"$dashboard_name"'`] | [0].DashboardId')
then
    if [ -z "$dashboardid" ] || [ "$dashboardid" = "null" ] || [ "$dashboardid" = "[]" ]
    then
    echo "No quicksight dashboard with name $dashboard_name"
    exit 1
    else
    echo "Dashboard ID for $dashboard_name is $dashboardid"

    analysis_arn=$(aws quicksight describe-dashboard --aws-account-id $aws_account_id --dashboard-id ${dashboardid//\"/} | jq -r ".Dashboard.Version.SourceEntityArn")
    analysis_id=$(echo "$analysis_arn" | cut -d'/' -f2)
    echo "Analysis ID for $dashboard_name is $analysis_id"

    dataset_arn=$(aws quicksight describe-dashboard --aws-account-id $aws_account_id --dashboard-id ${dashboardid//\"/} | jq -r ".Dashboard.Version.DataSetArns[0]")
    dataset_id=$(echo "$dataset_arn" | cut -d'/' -f2)
    echo "Dataset ID for $dashboard_name is $dataset_id"
    fi
else
echo "$dashboardid"
exit 1
fi


# Create Json file template.json
json_content=$(cat <<EOF
{
"SourceAnalysis": {
"Arn": "$analysis_arn",
"DataSetReferences": [
{
"DataSetPlaceholder": "$dataset_id",
"DataSetArn": "$dataset_arn"
}
]
}
}
EOF
)
echo "$json_content" > "template.json"

# Create Quicksight Template
template_id=$(uuidgen)
if aws quicksight create-template \
--aws-account-id $aws_account_id \
--region $aws_region \
--template-id $template_id \
--source-entity file://template.json ;
then
echo "create template succeded"
else
echo "create template failed"
exit 1
fi



# Create Json file template_permissions.json
templatearn=$(aws quicksight list-templates \
--aws-account-id $aws_account_id \
--region $aws_region \
--query 'reverse(sort_by(TemplateSummaryList,&LastUpdatedTime))[?TemplateId==`'"$template_id"'`] | [0].Arn')
echo "Template Arn is $templatearn"
echo "Template Id is $template_id"

json_content=$(cat <<EOF
[{
    "Principal": "arn:aws:iam::$DEV:root",
    "Actions": ["quicksight:UpdateTemplatePermissions", "quicksight:DescribeTemplate"]
},
{
    "Principal": "arn:aws:iam::$STG:root",
    "Actions": ["quicksight:UpdateTemplatePermissions", "quicksight:DescribeTemplate"]
},
{
    "Principal": "arn:aws:iam::$PROD:root",
    "Actions": ["quicksight:UpdateTemplatePermissions", "quicksight:DescribeTemplate"]
}]
EOF
)
echo "$json_content" > "template_permissions.json"

# Assigning destination account Quicksight Template permissions
echo $(aws quicksight update-template-permissions \
--aws-account-id $aws_account_id \
--region $aws_region \
--template-id $template_id \
--grant-permissions file://template_permissions.json)

# Creating .json with Quicksight Dataset description
mkdir -p "$dashboard_name"

aws quicksight describe-data-set \
--aws-account-id $aws_account_id \
--region $aws_region \
--data-set-id ${dataset_id//\"/} > "$dashboard_name/dataset_description.json"

aws quicksight describe-template \
--aws-account-id $aws_account_id \
--region $aws_region \
--template-id $template_id > "$dashboard_name/template_description.json"

rm template_permissions.json template.json