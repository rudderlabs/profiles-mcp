from tools.execution_backends import (
    BigQueryPbQueryStrategy,
    DatabricksPbQueryStrategy,
    RedshiftPbQueryStrategy,
    SnowflakePbQueryStrategy,
)


def test_snowflake_relation_name():
    strategy = SnowflakePbQueryStrategy()
    assert strategy.relation_name("DB", "SCHEMA", "TABLE") == "DB.SCHEMA.TABLE"


def test_snowflake_helper_queries():
    strategy = SnowflakePbQueryStrategy()

    assert strategy.describe_table_query("DB", "SCH", "TBL") == "DESCRIBE TABLE DB.SCH.TBL"
    assert strategy.list_tables_query("DB", "SCH") == "SHOW TABLES IN DB.SCH"

    top_q = strategy.top_events_query("DB", "SCH", "tracks")
    assert "FROM DB.SCH.tracks" in top_q
    assert "group by event" in top_q
    assert "limit 20" in top_q


def test_bigquery_relation_qualification_rules():
    strategy = BigQueryPbQueryStrategy()

    assert strategy.relation_name("my_project", "events", "tracks") == "my_project.events.tracks"
    assert strategy.relation_name("my_project", "events", "events.tracks") == "my_project.events.tracks"
    assert (
        strategy.relation_name("my_project", "events", "other_project.events.tracks")
        == "other_project.events.tracks"
    )


def test_bigquery_helper_queries_use_backticked_fully_qualified_references():
    strategy = BigQueryPbQueryStrategy()

    describe_q = strategy.describe_table_query("my_project", "events", "tracks")
    list_q = strategy.list_tables_query("my_project", "events")
    top_events_q = strategy.top_events_query("my_project", "events", "tracks")

    assert "`my_project.events.INFORMATION_SCHEMA.COLUMNS`" in describe_q
    assert "`my_project.events.INFORMATION_SCHEMA.TABLES`" in list_q
    assert "FROM `my_project.events.tracks`" in top_events_q


def test_databricks_unity_catalog_relation_names():
    uc_strategy = DatabricksPbQueryStrategy(catalog="main")
    non_uc_strategy = DatabricksPbQueryStrategy(catalog=None)

    assert uc_strategy.relation_name("ignored_db", "analytics", "tracks") == "main.analytics.tracks"
    assert uc_strategy.list_tables_query("ignored_db", "analytics") == "SHOW TABLES IN main.analytics"

    assert non_uc_strategy.relation_name("warehouse", "analytics", "tracks") == "warehouse.analytics.tracks"
    assert non_uc_strategy.relation_name("analytics", "analytics", "tracks") == "analytics.tracks"


def test_redshift_describe_uses_information_schema():
    strategy = RedshiftPbQueryStrategy()

    q = strategy.describe_table_query("dev", "public", "tracks")
    assert "information_schema.columns" in q
    assert "table_schema = 'public'" in q
    assert "table_name = 'tracks'" in q
    assert "ordinal_position" in q
