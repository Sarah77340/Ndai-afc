import os
import datetime as dt
import great_expectations as gx

def store_result(context, suite_name: str, result):
    store = context.validation_results_store

    run_name = "manual_run"
    run_time = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    batch_identifier = "batch"

    # ✅ construire la clé via la classe de clé du store
    key_cls = store.key_class  # ex: ValidationResultIdentifier
    key = key_cls.from_tuple((suite_name, run_name, run_time, batch_identifier))

    store.set(key, result)
    return key

def run_one(context, batch_request, suite_name: str):
    suite = context.suites.get(suite_name)
    validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)
    result = validator.validate()
    print(f"✅ Validation {suite_name}:", result.success)
    store_result(context, suite_name, result)
    return result

def main():
    root = os.path.abspath(os.path.dirname(__file__))
    context = gx.get_context(context_root_dir=os.path.join(root, "gx"))

    datasource = context.data_sources.get("postgres_datasource")

    sales_query = "SELECT * FROM sales"
    feedback_query = "SELECT * FROM campaign_feedback_enriched"

    assets = {a.name: a for a in datasource.assets}
    sales_asset = assets.get("sales_query") or datasource.add_query_asset("sales_query", sales_query)
    feedback_asset = assets.get("feedback_query") or datasource.add_query_asset("feedback_query", feedback_query)

    sales_batch_request = sales_asset.build_batch_request()
    feedback_batch_request = feedback_asset.build_batch_request()

    run_one(context, sales_batch_request, "sales_suite")
    run_one(context, feedback_batch_request, "feedback_suite")

    context.build_data_docs()
    print("✅ Data Docs générés.")
    print("📄 Ouvre :", os.path.join(root, "gx", "uncommitted", "data_docs", "local_site", "index.html"))

if __name__ == "__main__":
    main()