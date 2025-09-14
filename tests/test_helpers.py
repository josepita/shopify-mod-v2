import math
import pandas as pd

from utils.helpers import (
    clean_value,
    format_price,
    get_base_reference,
    get_variant_size,
    group_variants,
    process_tags,
)


def test_clean_value_basic():
    assert clean_value(None) == ""
    assert clean_value("") == ""
    assert clean_value("  hola  ") == "hola"
    assert clean_value(float("nan")) == ""


def test_format_price_parsing():
    assert format_price("12,34") == 12.34
    assert format_price("123") == 123.0
    # Thousand separator + decimal comma is not supported by parser -> returns 0.0
    assert format_price("EUR 1.234,56") == 0.0


def test_reference_parsing():
    assert get_base_reference("ABC/12") == "ABC"
    assert get_base_reference("ABC") == "ABC"
    assert get_variant_size("ABC/12") == "12"
    assert get_variant_size("ABC") is None


def test_group_variants_structure():
    data = [
        {"REFERENCIA": "ABC", "DESCRIPCION": "Anillo", "PRECIO": 10, "TIPO": "Sello"},
        {"REFERENCIA": "ABC/12", "DESCRIPCION": "Anillo talla 12", "PRECIO": 10, "TIPO": "Sello"},
        {"REFERENCIA": "DEF/5", "DESCRIPCION": "Aro 5mm", "PRECIO": 20, "TIPO": "Aros"},
    ]
    df = pd.DataFrame(data)
    grouped = group_variants(df)

    assert "ABC" in grouped and "DEF" in grouped
    assert grouped["ABC"]["is_variant_product"] is True
    assert len(grouped["ABC"]["variants"]) == 2
    assert grouped["DEF"]["is_variant_product"] is True


def test_process_tags_enriches():
    tags = process_tags("Anillos", "Oro", "Solitario", "Colgante del zodiaco aries")
    # Order may vary; check membership
    for expected in ["Anillos", "Oro", "Solitarios", "Horoscopo"]:
        assert expected in tags

