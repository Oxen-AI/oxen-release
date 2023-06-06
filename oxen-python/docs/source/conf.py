# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import toml
import sys, os

with open("../../Cargo.toml") as f:
    data = toml.load(f)

version = data["package"]["version"]

project = "🐂 🌾 Oxen"
copyright = "2023, OxenAI"
author = "Greg Schoeninger"
release = version

html_title = project + " " + version
html_last_updated_fmt = "%b %d, %Y"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "m2r2",
    "sphinx_copybutton",
    "sphinx.ext.autodoc",
    "sphinx.ext.autodoc.typehints",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
]

templates_path = ["_templates"]
exclude_patterns = []

copybutton_prompt_text = "$ "

source_suffix = [".md", ".rst"]

m2r_parse_relative_links = True

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_book_theme"
# html_static_path = ["_static"]
