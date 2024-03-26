import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col


def main(session: snowpark.Session):
    #
    #  User Editable variables.
    #
    your_database_and_schema_name = "SF_MAGNITE_TEST1_SHARE.PUBLIC"
    your_taxonomy_view_name = "TAXONOMY_SECURE_VIEW"
    your_membership_view_name = "MEMBERSHIP_SECURE_VIEW"

    # Full Paths to Views
    full_taxonomy_path = your_database_and_schema_name + "." + your_taxonomy_view_name
    full_membership_path = your_database_and_schema_name + "." + your_membership_view_name

    #
    #  Check Schema is as expected
    #
    expected_schema_membership_column_list = [
        """StructField('STORAGEID', StringType(255), nullable=False)""",
        """StructField('IDTYPE', StringType(36), nullable=False)""",
        """StructField('SEGMENTID', StringType(36), nullable=False)""",
        """StructField('ACTIVE', BooleanType(), nullable=False)""",
        """StructField('UPDATETIMESTAMP', TimestampType(tz=ntz), nullable=False)"""
    ]

    schema_membership = session.table(full_membership_path).schema
    for column in expected_schema_membership_column_list:
        if column not in str(schema_membership):
            return f"error: Your membership schema does not match the expected view name schema, missing or malformed. The expected column is: <{column}>, Your Full schema is {str(schema_membership)} "

    expected_schema_taxonomy_column_list = [
        """StructField('CLIENTNAME', StringType(255), nullable=False)""",
        """StructField('ACCOUNTID', StringType(36), nullable=False)""",
        """StructField('DATASHARESETTINGS', StringType(36), nullable=False)""",
        """StructField('SEGMENTID', StringType(36), nullable=False)""",
        """StructField('SEGMENTNAME', StringType(255), nullable=False)""",
        """StructField('SEGMENTDESCRIPTION', StringType(255), nullable=True)""",
        """StructField('SEGMENTCPM', DoubleType(), nullable=True)""",
        """StructField('PLATFORM', StringType(255), nullable=False)""",
        """StructField('ACTIVE', BooleanType(), nullable=False)""",
        """StructField('UPDATETIMESTAMP', TimestampType(tz=ntz), nullable=False)"""
    ]

    schema_taxonomy = session.table(full_taxonomy_path).schema
    for column in expected_schema_taxonomy_column_list:
        if column not in str(schema_taxonomy):
            return f"error: Your taxonomy schema does not match the expected view name schema. The expected column is: <{column}>:  Your Full schema is {str(schema_taxonomy)} "

    #
    # Check Views are SECURE VIEW
    #
    if "_SECURE_VIEW" not in your_taxonomy_view_name:
        return "Your taxonomy view name is invalid should be: XXXX_SECURE_VIEW"

    if "_SECURE_VIEW" not in your_membership_view_name:
        return "Your membership view name is invalid, include XXXX_SECURE_VIEW"

    membership_secure_view = session.table('information_schema.views').filter(
        col('table_name') == your_membership_view_name)
    taxonomy_secure_view = session.table('information_schema.views').filter(
        col('table_name') == your_taxonomy_view_name)

    table_schema_is_secure_membership = membership_secure_view.select("IS_SECURE").first()
    if table_schema_is_secure_membership is None:
        return f"error: Your membership view is not a SECURE VIEW, run: ALTER VIEW {your_membership_view_name} SET SECURE;"

    if table_schema_is_secure_membership.IS_SECURE != 'YES':
        return f"error: Your membership view is not a SECURE VIEW, run: ALTER VIEW {your_membership_view_name} SET SECURE;"

    table_schema_is_secure_taxonomy = taxonomy_secure_view.select("IS_SECURE").first()
    if table_schema_is_secure_taxonomy is None:
        return f"error: Your taxonomy view is not a SECURE VIEW, run: ALTER VIEW {your_taxonomy_view_name} SET SECURE;"

    if table_schema_is_secure_taxonomy.IS_SECURE != 'YES':
        return f"error: Your taxonomy view is not a SECURE VIEW, run: ALTER VIEW {your_taxonomy_view_name} SET SECURE;"

    #
    # Check CHANGE_TRACKING = ON
    #
    show_view_membership = session.sql(f"""SHOW VIEWS LIKE '{your_membership_view_name}' IN {your_database_and_schema_name};""").collect()[0][
        "change_tracking"]

    if show_view_membership != 'ON':
        return f"error: Your membership view does not have CHANGE_TRACKING = ON, run: alter view {your_membership_view_name} set CHANGE_TRACKING = TRUE;"

    show_view_taxonomy = session.sql(f"""SHOW VIEWS LIKE '{your_taxonomy_view_name}' IN {your_database_and_schema_name};""").collect()[0][
        "change_tracking"]

    if show_view_taxonomy != 'ON':
        return f"error: Your taxonomy view does not have CHANGE_TRACKING = ON, run: alter view {your_taxonomy_view_name} set CHANGE_TRACKING = TRUE;"

    return "success"