"""Shared link to open ChatGPT outside the Streamlit application."""

import streamlit as st


def render_chatgpt_link() -> None:
    """Render a compact ChatGPT link at the top-right of a page."""
    spacer, button_column = st.columns([4, 1])
    with button_column:
        st.link_button(
            "💬 Open ChatGPT",
            "https://chatgpt.com/",
            help="Open ChatGPT in a new browser tab",
            use_container_width=True,
        )
