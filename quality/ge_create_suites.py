import os
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite

def save_suite(context, suite):
    """
    Sauvegarde compatible selon les variantes GE 1.x
    """
    # certains environnements ont context.suites.save(...)
    if hasattr(context.suites, "save"):
        context.suites.save(suite)
        return
    # d'autres ont context.suites.add(...) pour enregistrer
    context.suites.add(suite)

def main():
    root = os.path.abspath(os.path.dirname(__file__))
    context = gx.get_context(context_root_dir=os.path.join(root, "gx"))

    # Supprime si déjà existant
    for suite_name in ["sales_suite", "feedback_suite"]:
        try:
            context.suites.delete(suite_name)
        except Exception:
            pass

    # -------- sales_suite --------
    sales_suite = ExpectationSuite(name="sales_suite")

    sales_suite.add_expectation(gx.expectations.ExpectColumnToExist(column="sale_date"))
    sales_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="sale_date"))
    sales_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="quantity", min_value=1))
    sales_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="total_amount", min_value=0))

    save_suite(context, sales_suite)
    print("sales_suite créée")

    # -------- feedback_suite --------
    feedback_suite = ExpectationSuite(name="feedback_suite")

    feedback_suite.add_expectation(gx.expectations.ExpectColumnToExist(column="campaign_id"))
    feedback_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="campaign_id"))
    feedback_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="sentiment_label",
            value_set=["positive", "neutral", "negative"],
        )
    )

    save_suite(context, feedback_suite)
    print("feedback_suite créée")

if __name__ == "__main__":
    main()