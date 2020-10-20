from enum import Enum


class DataTypes(Enum):
    STRING = "string"
    INT = "int"
    FLOAT = "float"
    DATE = "date"
    BOOLEAN = "boolean"


class GlueDataTypes(Enum):
    BIGINT = "bigint"
    BOOLEAN = "boolean"
    CHAR = "char"
    DATE = "date"
    DECIMAL = "decimal"
    DOUBLE = "double"
    FLOAT = "float"
    INT = "int"
    SMALLINT = "smallint"
    STRING = "string"
    TIMESTAMP = "timestamp"
    TINYINT = "tinyint"
    VARCHAR = "varchar"


class LynxDataTypeNames(Enum):
    NAME = "Name"
    ADDRESS = "Address"
    ZIP_CODE = "Zip Code"
    DATE = "Date"
    AGE = "Age"
    PHONE_NUMBER = "Phone Number"
    FAX_NUMBER = "Fax Number"
    EMAIL = "Email Address"
    SSN = "Social Security Number"
    MRD = "Medical Record Number"
    HPBN = "Health Plan Beneficiary Number"
    ACCOUNT_NUMBER = "Account Number"
    CERTIFICATE_NUMBER = "Certificate or License Number"
    IP_ADDRESS = "IP Address"
    NUMBER = "Number"
    TEXT = "Text"
    BOOLEAN = "Boolean"
    VIDSN = "Vehicle Identifier or Serial Number"
    DIDSN = "Device Identifier or Serial Number"
    WURL = "Web Universal Resource Locator"
    UID = "Unique Identifier"
    BIRTH_DATE = "Birth Date"


class Actions(Enum):
    OMIT = "omit"
    OFFSET = "offset"
    RANDOM_OFFSET = "random_offset"
    MASK = "mask"
    SALTED_HASH = "salted_hash"
    FREE_TEXT_REPLACEMENT = "free_text_replacement"
    LOWER_RESOLUTION = "lower_resolution"
