import os
import great_expectations as gx

def main():
    root = os.path.abspath(os.path.dirname(__file__))
    context = gx.get_context(context_root_dir=os.path.join(root, "gx"))

    conn = "postgresql+psycopg2://ndai:ndai@localhost:5432/ndai"

    try:
        context.sources.delete("postgres_datasource")
    except Exception:
        pass

    context.sources.add_sql(
        name="postgres_datasource",
        connection_string=conn,
    )

    context.save()
    print("Datasource postgres_datasource ajoutée.")

if __name__ == "__main__":
    main()