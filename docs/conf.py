"""Sphinx configuration."""

import configparser
import os

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "..", "setup.cfg"))

project = config.get("metadata", "name")
version = config.get("metadata", "version")
release = version  # Sphinx uses 'release' as the full version
author = "Kaleb Coberly"
email = "kaleb.coberly@gmail.com"
copyright = "Copyright (c) 2024 Kaleb Coberly"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    "sphinx_click",
]
html_theme = "furo"
html_context = {"version": version, "display_version": True}
