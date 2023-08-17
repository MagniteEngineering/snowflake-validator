import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session: snowpark.Session):
    # CUSTOMER INPUT REQUIRED
    #
    # Update PROVIDERDB.PUBLIC with your Database Name & Schema.
    #
    database_name_and_schema = str('PROVIDERDB.PUBLIC').upper()

    tableName = 'information_schema.views'

    build_expected_secure_view_name_membership = "MEMBERSHIP_SECURE_VIEW"
    build_expected_secure_view_name_taxonomy = "TAXONOMY_SECURE_VIEW"

    membership_secure_view = session.table(tableName).filter(col("TABLE_NAME") == build_expected_secure_view_name_membership)
    taxonomy_secure_view = session.table(tableName).filter(col("TABLE_NAME") == build_expected_secure_view_name_taxonomy)
    
    is_membership_secure_view = membership_secure_view.count()
    is_taxonomy_secure_view = taxonomy_secure_view.count()

    # Check Tables Exist
    if (is_membership_secure_view != 1):
        return f"error: Your membership view does not exist or match the expected view name <{build_expected_secure_view_name_membership}>"

    if (is_taxonomy_secure_view != 1):
       return f"error: Your taxonomy view does not exist or match the expected view name <{build_expected_secure_view_name_taxonomy}>"

    #
    # Check schemas are Correct
    #
    expected_schema_membership_column_list = [
        """StructField('STORAGEID', StringType(), nullable=False)""",
        """StructField('IDTYPE', StringType(), nullable=False)""",
        """StructField('SEGMENTID', StringType(), nullable=False)""",
        """StructField('ACTIVE', BooleanType(), nullable=False)""",
        """StructField('UPDATETIMESTAMP', TimestampType(), nullable=False)"""
    ]

    schema_membership = session.table(build_expected_secure_view_name_membership).schema
    for column in expected_schema_membership_column_list:
        if (column not in str(schema_membership)):
           return f"error: Your membership schema does not match the expected view name schema, missing or malformed. The expected column is: <{column}>: "
        
    expected_schema_taxonomy_column_list = [
        """StructField('CLIENTNAME', StringType(), nullable=False)""",
        """StructField('ACCOUNTID', StringType(), nullable=False)""",
        """StructField('DATASHARESETTINGS', StringType(), nullable=False)""",
        """StructField('SEGMENTID', StringType(), nullable=False)""",
        """StructField('SEGMENTNAME', StringType(), nullable=False)""",
        """StructField('SEGMENTDESCRIPTION', StringType(), nullable=True)""",
        """StructField('SEGMENTCPM', DoubleType(), nullable=True)""",
        """StructField('PLATFORM', StringType(), nullable=False)""",
        """StructField('ACTIVE', BooleanType(), nullable=False)""",
        """StructField('UPDATETIMESTAMP', TimestampType(), nullable=False)"""
    ]
    
    schema_taxonomy = session.table(build_expected_secure_view_name_taxonomy).schema
    for column in expected_schema_taxonomy_column_list:
        if (column not in str(schema_taxonomy)):
           return f"error: Your taxonomy schema does not match the expected view name schema. The expected column is: <{column}>: "

    #
    # Check Views are SECURE VIEW
    #
    table_schema_is_secure_membership = membership_secure_view.select("IS_SECURE").first().IS_SECURE
    if (table_schema_is_secure_membership != 'YES'):
        return f"error: Your membership view is not a SECURE VIEW, run: ALTER VIEW {build_expected_secure_view_name_membership} SET SECURE;"
        
    table_schema_is_secure_taxonomy = taxonomy_secure_view.select("IS_SECURE").first().IS_SECURE
    if (table_schema_is_secure_taxonomy != 'YES'):
        return f"error: Your taxonomy view is not a SECURE VIEW, run: ALTER VIEW {build_expected_secure_view_name_taxonomy} SET SECURE;"

    #
    # Check CHANGE_TRACKING = ON
    #
    show_view_membership = session.sql(f"""SHOW VIEWS LIKE '{build_expected_secure_view_name_membership}' IN {database_name_and_schema};""").collect()[0]["change_tracking"]

    if (show_view_membership != 'ON'):
        return f"error: Your membership view does not have CHANGE_TRACKING = ON, run: alter view {build_expected_secure_view_name_membership} set CHANGE_TRACKING = TRUE;"

    show_view_taxonomy = session.sql(f"""SHOW VIEWS LIKE '{build_expected_secure_view_name_taxonomy}' IN {database_name_and_schema};""").collect()[0]["change_tracking"]

    if (show_view_taxonomy != 'ON'):
        return f"error: Your taxonomy view does not have CHANGE_TRACKING = ON, run: alter view {build_expected_secure_view_name_taxonomy} set CHANGE_TRACKING = TRUE;"

    return "success"
