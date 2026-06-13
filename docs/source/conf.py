"""Configuration file for the Sphinx documentation builder.

For the full list of built-in configuration values, see the documentation:
https://www.sphinx-doc.org/en/master/usage/configuration.html
"""

import os
import sys

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'pygcp'
copyright = '2024'
author = 'Colin Dietrich'
version = '0.4.1'
release = '0.4'
html_title = 'PyGCP'  # top right text for link to site root

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'nbsphinx',
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'numpydoc',
    'myst_parser',
    'sphinx.ext.mathjax'
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

# -- Autodoc configurations --------------------
autodoc_type_aliases = {}
autodoc_docstring_signature = True

# -- path to source code being documented --------------------
abs_docs = os.path.abspath(os.path.dirname(__file__))
abs_code = os.path.abspath(os.path.join(abs_docs, "..", ".."))
sys.path.insert(0, abs_code)

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'pydata_sphinx_theme'
html_static_path = ['_static']

# -- Overall Config --
nitpicky = True


# -- Docstring Removal --
def remove_module_docstring(app, what, name, obj, options, lines):
    """Remove module docstrings."""
    include_modules = []  # modules to INCLUDE their module docstrings
    if what == "module" and name not in include_modules:
        del lines[:]


def setup(app):
    """All actions to take on setup."""
    app.connect("autodoc-process-docstring", remove_module_docstring)
