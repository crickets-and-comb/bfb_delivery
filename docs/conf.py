"""Sphinx configuration."""

import configparser
import os

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "..", "setup.cfg"))

project = config.get("metadata", "name")
version = config.get("metadata", "version")
release = version  # Sphinx uses 'release' as the full version
author = config.get("metadata", "author")
email = config.get("metadata", "maintainer_email")
copyright = config.get("metadata", "copyright")

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    "sphinx_click",
    "sphinxcontrib.mermaid",
]
html_theme = "furo"
html_context = {"version": version, "display_version": True}

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "private-members": True,
    "show-inheritance": True,
}
autodoc_class_signature = "separated"
