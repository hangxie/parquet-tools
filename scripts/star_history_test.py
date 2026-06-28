# -*- coding: utf-8 -*-
import contextlib
import importlib.util
import io
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).with_name("star-history.py")
SPEC = importlib.util.spec_from_file_location("star_history", SCRIPT_PATH)
star_history = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(star_history)


class GenerateHTMLTest(unittest.TestCase):
    def render_html(self):
        monthly = [
            ("2024-01-31", 5),
            ("2024-02-29", 8),
            ("2024-03-31", 13),
        ]
        cumulative = {
            "2024-01-31": 5,
            "2024-02-29": 8,
            "2024-03-30": 12,
            "2024-03-31": 13,
        }
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "star-history.html"
            with contextlib.redirect_stdout(io.StringIO()):
                star_history.generate_html(monthly, cumulative, output_path, recent_days=3)
            return output_path.read_text()

    def test_svg_scales_to_viewport(self):
        html = self.render_html()

        self.assertIn('<main class="chart-page">', html)
        self.assertIn('<svg class="chart" viewBox="0 0 900 420"', html)
        self.assertNotIn('<svg width="900" height="420">', html)
        self.assertIn("min-height: 100vh;", html)
        self.assertIn("width: min(100%, 175vh);", html)

    def test_hover_uses_scaled_svg_coordinates(self):
        html = self.render_html()

        self.assertIn("const svg = zone.closest('svg');", html)
        self.assertIn("svg.viewBox.baseVal.width", html)
        self.assertIn("const mx = rect.width", html)


if __name__ == "__main__":
    unittest.main()
