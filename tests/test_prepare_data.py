"""Unit tests for the local preparation helpers.

Run with:  python -m pytest tests/ -v
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local_prep"))

from prepare_data import IMDB_ID_RE, sanitize_name


class TestSanitizeName:
    def test_simple_name(self):
        assert sanitize_name("Nume Film", "H") == "nume_film"

    def test_strips_diacritics(self):
        assert sanitize_name("Observații 3", "J") == "observatii_3"

    def test_punctuation_collapses_to_underscore(self):
        assert sanitize_name("nr.", "F") == "nr"
        assert sanitize_name("Alcohol, Drugs", "AH") == "alcohol_drugs"

    def test_empty_header_falls_back_to_column_letter(self):
        assert sanitize_name(None, "A") == "col_a"
        assert sanitize_name("   ", "B") == "col_b"


class TestImdbIdRegex:
    def test_extracts_id_from_url(self):
        url = "https://www.imdb.com/title/tt0111161/?ref_=sr_t_1"
        assert IMDB_ID_RE.search(url).group(1) == "tt0111161"

    def test_no_match_without_tt_id(self):
        assert IMDB_ID_RE.search("https://example.com/movie/123") is None
