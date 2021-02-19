import contextlib
from io import StringIO
from typing import Generator, List

from django.utils import timezone
from django.db import connection, models


class BulkUpdateMixin(models.Model):
    """
    Использование:

    class TestModel(BulkUpdateMixin):
        field_1 = models.IntegerField()
        field_2 = models.CharField(max_length=200)
        field_3 = models.BooleanField()

    obj_1 = Test(id=1, field_1=1, field_2="1", field_3=True)
    obj_2 = Test(id=2, field_1=2, field_2="2", field_3=True)
    obj_3 = Test(id=3, field_1=3, field_2="3", field_3=True)
    Test.bulk_upsert([obj_1, obj_2, obj_3])

    obj_1 = Test(id=1, field_1=3, field_2="3", field_3=False)
    obj_2 = Test(id=2, field_1=1, field_2="1", field_3=False)
    obj_3 = Test(id=3, field_1=2, field_2="2", field_3=False)
    Test.bulk_upsert([obj_1, obj_2, obj_3])
    """

    class Meta:
        abstract = True

    @classmethod
    @contextlib.contextmanager
    def __setup_teardown_temp_tables(cls, cursor):
        cursor.execute(
            f"""
            DROP TABLE IF EXISTS temp_{cls._meta.db_table};

            CREATE TEMPORARY TABLE temp_{cls._meta.db_table} AS
            SELECT * FROM {cls._meta.db_table} LIMIT 0;
            """
        )
        try:
            yield
        finally:
            cursor.execute(
                f"""
                DROP TABLE IF EXISTS temp_{cls._meta.db_table};
                """
            )

    @classmethod
    def __create_tsv_file(cls, rows: Generator) -> StringIO:
        file = StringIO()
        for row in rows:
            file.write("\t".join(str(value) for value in row) + "\n")
        file.seek(0)
        return file

    @classmethod
    def __generate_rows(cls, objs: List[models.Model], fields: List[str]):
        for obj in objs:
            yield (getattr(obj, field) for field in fields)

    @classmethod
    def __populate_temp_table(cls, cursor, objs: List[models.Model], fields: List[str]):
        tsv_file = cls.__create_tsv_file(cls.__generate_rows(objs, fields))
        cursor.copy_from(
            tsv_file, f"temp_{cls._meta.db_table}", columns=fields, null="None",
        )

    @classmethod
    def __copy_from_temp_table(cls, cursor, fields: List[str]):
        cursor.execute(
            f"""
            INSERT INTO {cls._meta.db_table} ({",".join(fields)})
            SELECT {",".join(fields)}
            FROM temp_{cls._meta.db_table}
            ON CONFLICT({cls._meta.pk.name}) DO UPDATE SET
                {','.join([f"{col}=EXCLUDED.{col}" for col in fields])}
            """
        )

    @classmethod
    def bulk_upsert(cls, objs: List[models.Model]):
        fields = [
            f"{field.name}_id" if field.remote_field else field.name
            for field in cls._meta.fields
        ]
        with connection.cursor() as cursor:
            with cls.__setup_teardown_temp_tables(cursor):
                cls.__populate_temp_table(cursor, objs, fields)
                cls.__copy_from_temp_table(cursor, fields)