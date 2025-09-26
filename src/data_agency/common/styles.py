from IPython.display import display, HTML

# 1. Define the custom CSS to increase font sizes.
# The CSS targets markdown elements within the standard Jupyter output div.
# You can adjust the 'font-size' values as needed.


def apply_custom_styles():
    style = """
    <style>
        .jp-RenderedMarkdown p, .jp-RenderedMarkdown li {
            font-size: 12px; /* Adjust paragraph and list item font size */
        }
        .jp-RenderedMarkdown h1 {
            font-size: 24px; /* Adjust H1 font size */
        }
        .jp-RenderedMarkdown h2 {
            font-size: 20px; /* Adjust H2 font size */
        }
    </style>
    """
    display(HTML(style))
