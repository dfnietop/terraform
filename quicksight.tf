# resource "aws_quicksight_data_source" "athena" {
#   data_source_id = "${var.proyecto}-${var.dominio}-${var.ambiente}-athena-qs-dsrc"
#   name           = "${var.proyecto}-${var.dominio}-${var.ambiente}-athena-qs-dsrc"

#   parameters {
#     athena {
#         work_group = "primary"
#     }
#   }
#   ssl_properties {
#     disable_ssl = false
#   }
#   type = "ATHENA"
# }

# resource "aws_quicksight_data_set" "oferta_demanda" {
#   data_set_id = "${var.dominio}oferta-demanda-1"
#   name        = "${var.dominio}oferta-demanda-1"
#   import_mode = "DIRECT_QUERY"

#   dynamic physical_table_map {
#     for_each = local.quicksight.oferta_demanda.dataset_description.DataSet.PhysicalTableMap
#     content {
#       physical_table_map_id = physical_table_map.key
#       relational_table {
#         data_source_arn = aws_quicksight_data_source.athena.arn
#         catalog = physical_table_map.value.RelationalTable.Catalog
#         schema = physical_table_map.value.RelationalTable.Schema
#         name = physical_table_map.value.RelationalTable.Name
#         dynamic input_columns {
#           for_each = physical_table_map.value.RelationalTable.InputColumns
#           content {
#             name = input_columns.value.Name
#             type = input_columns.value.Type
#           }
#         }
#       }
#     }
#   }
#   dynamic logical_table_map {
#     for_each = local.quicksight.oferta_demanda.dataset_description.DataSet.LogicalTableMap
#     content {
#       logical_table_map_id = logical_table_map.key
#       alias = logical_table_map.value.Alias

#       source {
#         data_set_arn = lookup(logical_table_map.value.Source, "DataSetArn", null)
#         physical_table_id = lookup(logical_table_map.value.Source, "PhysicalTableId", null)
#         dynamic join_instruction {
#           for_each = lookup(logical_table_map.value.Source, "JoinInstruction", null) == null ? [] : [true]
#           content {
#             left_operand = lookup(logical_table_map.value.Source.JoinInstruction, "LeftOperand", null)
#             right_operand = lookup(logical_table_map.value.Source.JoinInstruction, "RightOperand", null)
#             type = lookup(logical_table_map.value.Source.JoinInstruction, "Type", null)
#             on_clause = lookup(logical_table_map.value.Source.JoinInstruction, "OnClause", null)
#           }
#         }
#       }
#       dynamic data_transforms {
#         for_each = logical_table_map.value.DataTransforms
#         content {
#           dynamic cast_column_type_operation {
#             for_each = [for item in data_transforms: item.CastColumnTypeOperation if can(item.CastColumnTypeOperation)]
#             content {
#               column_name = lookup(cast_column_type_operation.value, "ColumnName", null)
#               new_column_type = lookup(cast_column_type_operation.value, "NewColumnType", null)
#               # format = lookup(cast_column_type_operation.value, "Format", null)
#             }
#           }
#           dynamic rename_column_operation {
#             for_each = [for item in data_transforms: item.RenameColumnOperation if can(item.RenameColumnOperation)]
#             content {
#               column_name = lookup(rename_column_operation.value, "ColumnName", null)
#               new_column_name = lookup(rename_column_operation.value, "NewColumnName", null)
#             }
#           }
#           dynamic create_columns_operation {
#             for_each = [for item in data_transforms: item.CreateColumnsOperation if can(item.CreateColumnsOperation)]
#             content {
#               columns {
#                 column_id = lookup(create_columns_operation.value.Columns[0], "ColumnId", null)
#                 column_name = lookup(create_columns_operation.value.Columns[0], "ColumnName", null)
#                 expression = lookup(create_columns_operation.value.Columns[0], "Expression", null)
#               }
#             }
#           }
#           dynamic tag_column_operation {
#             for_each = [for item in data_transforms: item.TagColumnOperation if can(item.TagColumnOperation)]
#             content {
#               column_name = lookup(tag_column_operation.value, "ColumnName", null)
#               tags {
#                 # column_description {
#                 #   text = lookup(tag_column_operation.value.ColumnDescription, "Text", null)
#                 # }
#                 column_geographic_role = lookup(tag_column_operation.value.Tags[0], "ColumnGeographicRole", null)
#               }
#             }
#           }
#           dynamic project_operation {
#             for_each = [for item in data_transforms: item.ProjectOperation if can(item.ProjectOperation)]
#             content {
#               projected_columns = lookup(project_operation.value, "ProjectedColumns", null)
#             }
#           }
#         }
#       }
#         # filter_operation {
          
#         # }
#         # untag_column_operation {
          
#         # }
#       # relational_table {
#       #   data_source_arn = aws_quicksight_data_source.athena.arn
#       #   catalog = physical_table_map.value.RelationalTable.Catalog
#       #   schema = physical_table_map.value.RelationalTable.Schema
#       #   name = physical_table_map.value.RelationalTable.Name
#       #   dynamic input_columns {
#       #     for_each = physical_table_map.value.RelationalTable.InputColumns
#       #     content {
#       #       name = input_columns.value.Name
#       #       type = input_columns.value.Type
#       #     }
#       #   }
#       # }
#     }
#   }
#   dynamic column_groups {
#     for_each = local.quicksight.oferta_demanda.dataset_description.DataSet.ColumnGroups
#     content {
#       geo_spatial_column_group {
#         columns = ["ds_country"]
#         country_code = "US"
#         name = "ds_country"
#       }
#     }
#   }
#   permissions {
#     actions = [
#       "quicksight:PassDataSet",
#       "quicksight:DescribeIngestion",
#       "quicksight:CreateIngestion",
#       "quicksight:UpdateDataSet",
#       "quicksight:DeleteDataSet",
#       "quicksight:DescribeDataSet",
#       "quicksight:CancelIngestion",
#       "quicksight:DescribeDataSetPermissions",
#       "quicksight:ListIngestions",
#       "quicksight:UpdateDataSetPermissions",
#     ]
#     principal = var.quicksight_user_arn
#   }
#   # lifecycle {
#   #   ignore_changes = all
#   # }
# }