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

# ===== Título centrado =====
st.markdown(
    """
    <div style="display:flex;align-items:center;justify-content:center;margin:8px 0 12px 0;">
      <span style="font-size:36px;font-weight:800;">Publicación GS1 → EDI</span>
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

def yesno(val, default_yes=True):
    s = str(val).lower()
    if s in ("1", "true", "yes", "y", "si", "sí"):
        return "yes"
    if s in ("0", "false", "no"):
        return "no"
    return "yes" if default_yes else "no"

@st.cache_resource(show_spinner=False)
def get_engine_from_values(server, database, user, password, encrypt="yes", trust="yes"):
    """
    Engine SQLAlchemy mssql+pytds (sin ODBC), con TLS (encrypt/trustservercertificate).
    """
    url = (
        f"mssql+pytds://{user}:{password}@{server}:1433/{database}"
        f"?encrypt={yesno(encrypt)}&trustservercertificate={yesno(trust)}&autocommit=True"
    )
    engine = create_engine(url, pool_pre_ping=True, pool_recycle=180)
    # Sanity check
    with engine.begin() as conn:
        conn.execute(text("SELECT 1"))
    return engine

def secrets_available():
    return all(k in st.secrets for k in ("DB_SERVER","DB_NAME","DB_USER","DB_PASS"))

# ==============================================
# Login / Conexión (Cloud con secrets o Local con formulario)
# ==============================================
engine = None

if secrets_available():
    # Cloud: toma secrets y oculta sidebar
    server = st.secrets["DB_SERVER"]
    database = st.secrets["DB_NAME"]
    user = st.secrets["DB_USER"]
    password = st.secrets["DB_PASS"]
    encrypt = st.secrets.get("DB_ENCRYPT", "yes")
    trust   = st.secrets.get("DB_TRUST", "yes")
    try:
        engine = get_engine_from_values(server, database, user, password, encrypt, trust)
        st.markdown(HIDE_SIDEBAR_CSS, unsafe_allow_html=True)
    except Exception as e:
        st.error(f"No se pudo conectar con las credenciales de la nube: {e}")
        st.stop()
else:
    # Local: login con formulario (sin botón ojo) y ocultar luego
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False

    with st.sidebar:
        st.header('Login SQL Server')
        st.markdown(HIDE_PASSWORD_TOGGLE_CSS, unsafe_allow_html=True)
        with st.form('login_form', clear_on_submit=False):
            server = st.text_input('Servidor', value='ec2-18-210-23-246.compute-1.amazonaws.com')
            database = st.text_input('Base de datos', value='PortalIntegradoGS1BD')
            user = st.text_input('Usuario', value='')
            password = st.text_input('Password', type='password', value='')
            encrypt = st.checkbox('Encrypt=yes', value=True)
            trust = st.checkbox('TrustServerCertificate=yes (requerido por tu servidor)', value=True)
            submitted = st.form_submit_button('Conectar')

    if submitted:
        try:
            engine = get_engine_from_values(server, database, user, password, encrypt, trust)
            st.session_state.authenticated = True
            st.success('Conectado correctamente.')
        except Exception as e:
            st.session_state.authenticated = False
            st.error(f'Error de conexión: {e}')

    if not engine:
        st.info('Conéctate en la barra lateral para comenzar.')
        st.stop()
    else:
        if st.session_state.get('authenticated'):
            st.markdown(HIDE_SIDEBAR_CSS, unsafe_allow_html=True)

# ==============================================
# Filtros (debajo del semáforo, pero se definen aquí)
# ==============================================
if 'page' not in st.session_state:
    st.session_state.page = 1

filters_container = st.container()  # luego del semáforo

with filters_container:
    ctrl_left, ctrl_mid, ctrl_right = st.columns([1,1,2])
    with ctrl_left:
        page_size = st.selectbox('Filas por página', [50,100,200,500], index=3)  # 500 por defecto
    with ctrl_mid:
        plataformas = ['(todas)','EDI','AltaEmpresa','BajaEmpresa','AltaUsuario']
        plat_default = plataformas.index('EDI') if 'EDI' in plataformas else 0
        plataforma_sel = st.selectbox('Plataforma (server-side)', plataformas, index=plat_default)
    with ctrl_right:
        st.caption('Filtrando por defecto últimos 30 días (server-side).')

# Ventana temporal y parámetros
end_date = datetime.now().date() + timedelta(days=1)   # exclusivo
start_date = (datetime.now().date() - timedelta(days=30))
plat_param = None if plataforma_sel == '(todas)' else plataforma_sel

# ==============================================
# Consultas (COUNT + PAGE) via SQLAlchemy
# ==============================================
SQL_COUNT = """
    SELECT COUNT(*) AS total
    FROM [PortalIntegradoGS1BD].[dbo].[LegacyJobs] j
    LEFT JOIN [PortalIntegradoGS1BD].[dbo].[Empresas] e ON e.IdEmpresa = j.IdEmpresa
    WHERE j.FechaAlta >= :start AND j.FechaAlta < :end
      AND (:plat IS NULL OR j.Plataforma = :plat)
"""
SQL_PAGE = """
    SELECT j.Id, j.FechaAlta, j.Plataforma, j.Metodo,
           j.MotivoRechazo, j.IdEmpresa,
           e.CodEmpre, e.RazonSocial, e.CUIT
    FROM [PortalIntegradoGS1BD].[dbo].[LegacyJobs] j
    LEFT JOIN [PortalIntegradoGS1BD].[dbo].[Empresas] e ON e.IdEmpresa = j.IdEmpresa
    WHERE j.FechaAlta >= :start AND j.FechaAlta < :end
      AND (:plat IS NULL OR j.Plataforma = :plat)
    ORDER BY j.FechaAlta DESC
    OFFSET :off ROWS FETCH NEXT :psz ROWS ONLY;
"""

page = st.session_state.page
offset = max((page-1)*page_size, 0)

# ----- COUNT -----
with st.spinner('Calculando total…'):
    with engine.begin() as conn:
        total = conn.execute(
            text(SQL_COUNT),
            {"start": start_date, "end": end_date, "plat": plat_param}
        ).scalar() or 0

# ----- PAGE -----
with st.spinner('Cargando página de datos…'):
    with engine.begin() as conn:
        result = conn.execute(
            text(SQL_PAGE),
            {"start": start_date, "end": end_date, "plat": plat_param, "off": offset, "psz": page_size}
        )
        rows = result.fetchall()
        cols = list(result.keys())
        if rows:
            df = pd.DataFrame(rows, columns=cols)
        else:
            df = pd.DataFrame(columns=cols or [
                "Id","FechaAlta","Plataforma","Metodo","MotivoRechazo","IdEmpresa","CodEmpre","RazonSocial","CUIT"
            ])

# ==============================================
# Semáforo (se imprime ARRIBA del bloque de filtros)
# ==============================================
critical_pattern = r'(' \
                   r'Error al dar de alta la empresa' \
                   r'|Error en el alta de la empresa\.\s*-\s*Invalid argument supplied for foreach\(\)' \
                   r'|No existe la empresa, no se creo el usuario' \
                   r'|No existe el usuario, no se creo el usuario' \
                   r')'

crit_mask = df['MotivoRechazo'].astype(str).str.contains(critical_pattern, case=False, na=False)
crit_count = int(crit_mask.sum())
ok_count   = int(len(df) - crit_count)

with sem_container:
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(
            f"""
            <div style="
                padding:20px;border-radius:16px;background:#ff4d4f;
                color:white;font-weight:900;font-size:28px;text-align:center;
                box-shadow:0 8px 20px rgba(255,77,79,0.45); margin-bottom:14px;">
                🔴 ERROR: {crit_count}
            </div>
            """,
            unsafe_allow_html=True
        )
    with c2:
        st.markdown(
            f"""
            <div style="
                padding:20px;border-radius:16px;background:#06c1671a;
                color:#0e7a3f;font-weight:900;font-size:28px;text-align:center;border:3px solid #23c16b;
                box-shadow:0 8px 20px rgba(35,193,107,0.35); margin-bottom:14px;">
                🟢 OK: {ok_count}
            </div>
            """,
            unsafe_allow_html=True
        )

# ==============================================
# Métricas y toggles
# ==============================================
m1, m2, m3, m4 = st.columns(4)
with m1: st.metric('Total últimos 30 días', total)
with m2: st.metric('Página', page)
with m3: st.metric('Filas en página', len(df))
with m4: st.metric('Plataforma', plataforma_sel)

f1, f2 = st.columns(2)
with f1:
    show_only_crit = st.checkbox("Ver SOLO errores críticos (rojo)", value=False)
with f2:
    show_only_ok = st.checkbox("Ver SOLO OK (verde)", value=False)

if show_only_crit and show_only_ok:
    display_df = df
elif show_only_crit:
    display_df = df[crit_mask]
elif show_only_ok:
    display_df = df[~crit_mask]
else:
    display_df = df

# ==============================================
# Navegación
# ==============================================
nav1, nav2, nav3, nav4 = st.columns([1,1,3,3])
with nav1:
    if st.button('⬅️ Anterior', disabled=(page<=1)):
        st.session_state.page = max(page-1, 1)
        st.rerun()
with nav2:
    if st.button('Siguiente ➡️', disabled=(offset+page_size>=total)):
        st.session_state.page = page+1
        st.rerun()
with nav3:
    goto = st.number_input('Ir a página', min_value=1, max_value=max((total+page_size-1)//page_size,1), value=page, step=1)
with nav4:
    if st.button('Ir'):
        st.session_state.page = int(goto)
        st.rerun()

# ==============================================
# Tabla (filas completas coloreadas según semáforo)
# ==============================================
if not display_df.empty:
    # Mostrar "MotivoRechazo" como "Respuestas"
    display_df = display_df.rename(columns={"MotivoRechazo": "Respuestas"})
    display_df['FechaAlta'] = pd.to_datetime(display_df['FechaAlta'], errors='coerce')

    # Patrón crítico para filas (usar la columna ya renombrada)
    crit_re = re.compile(critical_pattern, re.IGNORECASE)

    def row_style(row: pd.Series):
        txt = str(row.get('Respuestas', '') or '')
        if crit_re.search(txt):
            # Fila crítica (ROJO)
            return ['background-color: #ff4d4f; color: white;'] * len(row)
        else:
            # Fila OK/resto (VERDE)
            return ['background-color: #eaffea; color: black;'] * len(row)

    cols_show = ['Id','FechaAlta','Plataforma','CodEmpre','RazonSocial','CUIT','Respuestas']
    # Asegurar que existan (por si la página vino vacía con columnas mínimas)
    cols_show = [c for c in cols_show if c in display_df.columns]

    styled_df = display_df[cols_show].style.apply(row_style, axis=1)
    st.dataframe(styled_df, use_container_width=True, hide_index=True)
else:
    st.warning('No hay resultados para los filtros actuales.')

st.markdown('---')

# ==============================================
# Detalle: XML por selección
# ==============================================
if not display_df.empty:
    left, right = st.columns([1,2])
    with left:
        job_id = st.selectbox('Selecciona un Job Id para ver su XML de Parametros', options=list(display_df['Id']))

    SQL_XML = """
        SELECT CAST(Parametros AS NVARCHAR(MAX)) AS ParametrosXml
        FROM [PortalIntegradoGS1BD].[dbo].[LegacyJobs]
        WHERE Id = :id;
    """
    xml_text = ''
    try:
        with engine.begin() as conn:
            result = conn.execute(text(SQL_XML), {"id": int(job_id)})
            row = result.fetchone()
            if row and row[0]:
                xml_text = str(row[0])
    except Exception as e:
        st.error(f'No se pudo obtener el XML: {e}')

    with right:
        st.subheader('XML de Parametros')
        if xml_text:
            st.code(prettify_xml(xml_text), language='xml')
        else:
            st.info('Selecciona un Job con Parametros disponibles para visualizar el XML.')
