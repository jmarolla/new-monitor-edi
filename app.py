import os
import re
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from xml.dom import minidom
from datetime import datetime, timedelta

# ==============================================
# Config general
# ==============================================
st.set_page_config(page_title='Publicación GS1 → EDI', layout='wide')

# ===== CSS GS1 (colores corporativos) =====
GS1_STYLE = """
<style>
body {
    background-color: #f5f5f5;
    color: #003366;
}

h1, h2, h3, h4, h5, h6 {
    color: #003366 !important;
}

[data-testid="stMetric"] {
    background: #ffffff;
    border: 2px solid #003366;
    border-radius: 16px;
    padding: 12px;
    color: #003366;
    font-weight: 600;
    box-shadow: 0 4px 12px rgba(0,0,0,0.1);
}

button[kind="primary"] {
    background-color: #FF6600 !important;
    color: white !important;
    border-radius: 12px !important;
    border: none !important;
    font-weight: 700 !important;
}

button[kind="secondary"] {
    background-color: #003366 !important;
    color: white !important;
    border-radius: 12px !important;
    font-weight: 700 !important;
}

.stSelectbox label, .stNumberInput label, .stCheckbox label, .stTextInput label {
    color: #003366 !important;
    font-weight: 600;
}

.stDataFrame thead tr th {
    background-color: #003366 !important;
    color: white !important;
    font-weight: 700;
}

.stDataFrame tbody tr td {
    border-bottom: 1px solid #ddd !important;
}

hr {
    border: 1px solid #003366;
}

/* Estilos login sidebar */
section[data-testid="stSidebar"] > div:first-child {
    background-color: #003366 !important;
    color: white !important;
    border-radius: 0 12px 12px 0;
    padding: 20px;
}

section[data-testid="stSidebar"] label {
    color: white !important;
    font-weight: 600;
}

section[data-testid="stSidebar"] input {
    border-radius: 8px;
    border: 1px solid #FF6600;
}

section[data-testid="stSidebar"] button[kind="primary"] {
    background-color: #FF6600 !important;
    color: white !important;
    font-weight: 700 !important;
}
</style>
"""
st.markdown(GS1_STYLE, unsafe_allow_html=True)

# ===== Título centrado =====
st.markdown(
    """
    <div style="display:flex;align-items:center;justify-content:center;margin:8px 0 12px 0;">
      <span style="font-size:36px;font-weight:800;color:#003366;">Publicación GS1 → EDI</span>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Contenedor para el semáforo (se llena más abajo) ---
sem_container = st.container()

# ===== CSS helpers =====
HIDE_SIDEBAR_CSS = """
<style>
[data-testid="stSidebar"] { display: none !important; }
[data-testid="collapsedControl"] { display: none !important; }
</style>
"""
HIDE_PASSWORD_TOGGLE_CSS = """
<style>
button[aria-label="Show password text"],
button[aria-label="Hide password text"] {
  display: none !important;
}
</style>
"""

# ==============================================
# Utils
# ==============================================
def prettify_xml(xml_text: str) -> str:
    try:
        return minidom.parseString(xml_text.encode('utf-8')).toprettyxml(indent='  ')
    except Exception:
        return xml_text

# ... (resto del código queda igual, sin cambios en la lógica de conexión, consultas, filtros, semáforo y tablas)
