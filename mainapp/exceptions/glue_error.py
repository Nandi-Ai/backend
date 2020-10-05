class GlueError(Exception):
    pass


class UnableToGetGlueColumns(GlueError):
    def __init__(self):
        super().__init__("Unable to get glue column")


class GlueTableFetchError(GlueError):
    def __init__(self, glue_database, glue_table):
        self.__glue_database = glue_database
        self.__glue_table = glue_table

    def __str__(self):
        return f"Error fetching glue table {self.__glue_table} in database {self.__glue_database}"


class GlueTableMigrationError(GlueError):
    def __init__(self, glue_database, original_table, new_table):
        self.__glue_database = glue_database
        self.__original_table = original_table
        self.__new_table = new_table

    def __str__(self):
        return (
            f"Failed to migrate table {self.__original_table} to {self.__new_table} "
            f"in database {self.__glue_database}"
        )
