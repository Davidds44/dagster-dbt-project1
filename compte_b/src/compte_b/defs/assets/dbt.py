import dagster as dg
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator
from compte_b.defs.project import dbt_project


class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "model":
            return "staging"
        return super().get_group_name(dbt_resource_props)

    def get_asset_key(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "source":
            # Map dbt sources directly to the upstream raw DuckDB assets.
            return dg.AssetKey(dbt_resource_props["name"])
        return super().get_asset_key(dbt_resource_props)

@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
)
def dbt_analytics(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
