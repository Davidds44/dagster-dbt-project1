import dagster as dg
from dagster_dbt import DbtCliResource, dbt_assets
from compte_b.defs.project import dbt_project



dbt_assets(
    manifest=dbt_project.manifest_path
)
def dbt_analysis(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


################à prendre en compte des que DBT commencera à ce construire en source de données #################
# import dagster as dg
# from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

# from dagster_and_dbt.defs.project import dbt_project


# class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
#     def get_asset_key(self, dbt_resource_props):
#         resource_type = dbt_resource_props["resource_type"]
#         name = dbt_resource_props["name"]
#         if resource_type == "source":
#             return dg.AssetKey(f"taxi_{name}")
#         else:
#             return super().get_asset_key(dbt_resource_props)

        
# @dbt_assets(
#     manifest=dbt_project.manifest_path, 
#     dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
# )
# def dbt_analytics(context: dg.AssetExecutionContext, dbt: DbtCliResource):
#     yield from dbt.cli(["build"], context=context).stream()