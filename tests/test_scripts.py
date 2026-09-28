import xml.etree.ElementTree as ET

from scripts.banner import render


def test_banner_is_valid_svg_and_escapes_text():
    svg = render(3, "CDC <Pipeline>", "a & b", ["Python", "PostgreSQL"])
    root = ET.fromstring(svg)
    assert root.tag.endswith("svg")
    assert "DAY 03 / 30" in svg
    assert "&lt;Pipeline&gt;" in svg
    assert svg.count("<rect") == 2 + 2  # background, accent bar, two chips


def test_banner_without_day_omits_counter():
    assert "DAY" not in render(None, "Hub", "", [])
