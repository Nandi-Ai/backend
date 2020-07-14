from django.test import TestCase
from mainapp.utils import lib
import os


class UtilsLibTestCase(TestCase):
    READ_FILE_PATH = "mainapp/tests/utills/test/test_csv_file.csv"
    WRITE_FILE_PATH = "mainapp/tests/utills/test/write_to_file.csv"

    def tearDown(self):
        os.remove(self.WRITE_FILE_PATH)

    def test_replace_missing_file_columns(self):
        lib.replace_empty_col_name_on_downloaded_file(
            read_file_path=self.READ_FILE_PATH,
            write_file_path=self.WRITE_FILE_PATH,
            delimiter=",",
        )

        with open(self.WRITE_FILE_PATH) as read_file:
            split_line = read_file.readline().split(",")
            for item in split_line:
                self.assertTrue(item != "")
