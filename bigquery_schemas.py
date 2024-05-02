from google.cloud import bigquery
from typing import Dict, List

schemas: Dict[str, List[bigquery.SchemaField]] = {
    "work": [
        bigquery.SchemaField("work_id", "INTEGER", mode="NULLABLE", description="PK"),
        bigquery.SchemaField("publisher_id", "INTEGER", mode="NULLABLE", description="FK publisher"),
        bigquery.SchemaField("subject_id", "INTEGER", mode="NULLABLE", description="FK subject"),
        bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("release_year", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("weight", "FLOAT", mode="NULLABLE"),
    ],
    "subject": [
        bigquery.SchemaField(
            "subject_id", "INTEGER", mode="NULLABLE", description="PK"
        ),
        bigquery.SchemaField("subject_name", "STRING", mode="NULLABLE"),
    ],
    "work_author": [
        bigquery.SchemaField("work_id", "INTEGER", mode="NULLABLE", description="PK FK work"),
        bigquery.SchemaField("author_id", "INTEGER", mode="NULLABLE", description="PK FK author"),
        bigquery.SchemaField("coefficient", "FLOAT", mode="NULLABLE"),
    ],
    "medium": [
        bigquery.SchemaField("medium_id", "INTEGER", mode="NULLABLE", description="PK"),
        bigquery.SchemaField("medium_name", "STRING", mode="NULLABLE"),
    ],
    "listing_type": [
        bigquery.SchemaField(
            "listing_type_id", "INTEGER", mode="NULLABLE", description="PK"
        ),
        bigquery.SchemaField("listing_type_name", "STRING", mode="NULLABLE"),
    ],
    "language": [
        bigquery.SchemaField(
            "language_id", "STRING", mode="NULLABLE", description="PK"
        ),
        bigquery.SchemaField("language_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("speakers", "INTEGER", mode="NULLABLE"),
    ],
    "date": [
        bigquery.SchemaField("year", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("month", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("quarter", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("date_id", "INTEGER", mode="NULLABLE", description="PK"),
    ],
    "user": [
        bigquery.SchemaField("user_id", "INTEGER", mode="NULLABLE", description="PK"),
        bigquery.SchemaField("age_group", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("gender", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("first_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("full_name", "STRING", mode="NULLABLE"),
    ],
    "return_fact": [
        bigquery.SchemaField("pages", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("items_left", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("work_age", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("reader_age", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("days_loaned", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("user_id", "INTEGER", mode="NULLABLE", description="PK FK user"),
        bigquery.SchemaField("date_id", "INTEGER", mode="NULLABLE", description="PK FK date"),
        bigquery.SchemaField("work_id", "INTEGER", mode="NULLABLE", description="PK FK work"),
        bigquery.SchemaField("medium_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("language_id", "STRING", mode="NULLABLE"),
    ],
    "rating_fact": [
        bigquery.SchemaField("pages", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("score", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("work_age", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("reader_age", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("user_id", "INTEGER", mode="NULLABLE", description="PK FK user"),
        bigquery.SchemaField("date_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("work_id", "INTEGER", mode="NULLABLE", description="PK FK work"),
        bigquery.SchemaField("language_id", "STRING", mode="NULLABLE"),
    ],
    "listing_fact": [
        bigquery.SchemaField("pages", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("work_age", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("reader_age", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("user_id", "INTEGER", mode="NULLABLE", description="PK FK user"),
        bigquery.SchemaField("date_id", "INTEGER", mode="NULLABLE", description="FK date"),
        bigquery.SchemaField("work_id", "INTEGER", mode="NULLABLE", description="PK FK work"),
        bigquery.SchemaField("language_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            "listing_type_id", "INTEGER", mode="NULLABLE", description="PK FK listing_type"
        ),
    ],
    "author": [
        bigquery.SchemaField("author_id", "INTEGER", mode="NULLABLE", description="PK"),
        bigquery.SchemaField("author_name", "STRING", mode="NULLABLE"),
    ],
    "publisher": [
        bigquery.SchemaField(
            "publisher_id", "INTEGER", mode="NULLABLE", description="PK"
        ),
        bigquery.SchemaField("publisher_name", "STRING", mode="NULLABLE"),
    ],
}
