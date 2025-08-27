"""
Unit tests for text extraction.

Call it like this:

python -m tests.test_extract_text
"""

import unittest
from bs4 import BeautifulSoup

from extract_text import HTML2Text

class TestHTML2Text(unittest.TestCase):

    def test_tagesschau(self):
        html2text = HTML2Text()

        with open("tests/assets/extract_text/tagesschau.de.html", "r", encoding="utf-8") as file:
            file_content = file.read()        
            soup = BeautifulSoup(file_content, "html.parser")
            result = html2text.extract_text(soup)

            # test some of the segments
            text = "AfD geht weiter gegen Verdachtsfall-Einstufung vor"
            self.assertTrue(text in result)

            text = "Die USA und die Europäische Union treiben die Umsetzung ihres Handelsabkommens voran. Beide Seiten veröffentlichten nun Details - demnach sollen etwa die US-Zölle auf Autoimporte auf 15 Prozent gesenkt werden."
            self.assertTrue(text in result)

            text = "Bundesregierung will ab 2026 Stromkunden entlasten"
            self.assertTrue(text in result)

            # make sure that some small navigation items are being filtered out
            text = "Inland"
            self.assertTrue(text not in result)

            text = "Ausland"
            self.assertTrue(text not in result)

            text = "Wirtschaft"
            self.assertTrue(text not in result)

if __name__ == '__main__':
    unittest.main()
