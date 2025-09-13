import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from xml.dom import minidom
from datetime import datetime, timedelta

# ==============================================
# Configuración General de la Página
# ==============================================
st.set_page_config(page_title='Publicación GS1 → EDI', layout='wide', initial_sidebar_state='expanded')

# ==============================================
# Estilos CSS
# ==============================================
# Estilos para las tarjetas de métricas (OK/Error), el título y otros helpers.
st.markdown("""
<style>
    /* Título principal */
    .main-title {
        font-size: 40px;
        font-weight: 700;
        text-align: center;
        margin-bottom: 24px;
    }

    /* Tarjetas para métricas de estado (OK/Error) */
    .metric-card {
        padding: 1.5rem;
        border-radius: 0.75rem;
        border: 1px solid;
        text-align: center;
        box-shadow: 0 4px 12px rgba(0,0,0,0.05);
    }
    .error-card {
        border-color: #ff4d4f;
        background-color: #ff4d4f15;
    }
    .ok-card {
        border-color: #23c16b;
        background-color: #06c16715;
    }
    .metric-card-title {
        font-size: 1rem;
        font-weight: 600;
        color: #4f4f4f;
        margin-bottom: 0.5rem;
    }
    .metric-card-value {
        font-size: 2.75rem;
        font-weight: 800;
        line-height: 1;
    }
    .error-text { color: #ff4d4f; }
    .ok-text { color: #23c16b; }

    /* Ocultar elementos de la UI */
    [data-testid="stSidebar"] { display: none !important; }
    [data-testid="collapsedControl"] { display: none !important; }
    button[aria-label="Show password text"],
    button[aria-label="Hide password text"] {
        display: none !important;
    }
</style>
""", unsafe_allow_html=True)

# ==============================================
# Funciones Utilitarias
# ==============================================
def prettify_xml(xml_text: str) -> str:
    """Formatea un string XML para una mejor legibilidad."""
    try:
        return minidom.parseString(xml_text.encode('utf-8')).toprettyxml(indent='  ')
    except Exception:
        return xml_text

def yesno(val, default_yes=True):
    """Convierte un valor a 'yes' o 'no'."""
    s = str(val).lower()
    if s in ("1", "true", "yes", "y", "si", "sí"):
        return "yes"
    if s in ("0", "false", "no"):
        return "no"
    return "yes" if default_yes else "no"

@st.cache_resource(show_spinner="Conectando a la base de datos...")
def get_engine_from_values(server, database, user, password, encrypt="yes", trust="yes"):
    """
    Crea un engine SQLAlchemy usando mssql+pytds.
    Soporta TLS con encrypt/trustservercertificate.
    """
    url = (
        f"mssql+pytds://{user}:{password}@{server}:1433/{database}"
        f"?encrypt={yesno(encrypt)}&trustservercertificate={yesno(trust)}&autocommit=True"
    )
    engine = create_engine(url, pool_pre_ping=True, pool_recycle=180)
    with engine.begin() as conn:
        conn.execute(text("SELECT 1"))  # Probar conexión
    return engine

def secrets_available():
    """Verifica si las credenciales de la DB están en st.secrets."""
    return all(k in st.secrets for k in ("DB_SERVER", "DB_NAME", "DB_USER", "DB_PASS"))

# ==============================================
# Lógica de Conexión
# ==============================================
engine = None
hide_sidebar_css = "<style>[data-testid='stSidebar'] { display: none !important; }</style>"

# Si hay secrets, se conecta automáticamente (entorno cloud)
if secrets_available():
    try:
        engine = get_engine_from_values(
            server=st.secrets["DB_SERVER"],
            database=st.secrets["DB_NAME"],
            user=st.secrets["DB_USER"],
            password=st.secrets["DB_PASS"],
            encrypt=st.secrets.get("DB_ENCRYPT", "yes"),
            trust=st.secrets.get("DB_TRUST", "yes")
        )
        st.markdown(hide_sidebar_css, unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Error en la conexión automática: {e}")
        st.stop()
# Si no, muestra el formulario de login en la sidebar (entorno local)
else:
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False

    with st.sidebar:
        st.header('Conexión a SQL Server')
        with st.form('login_form'):
            server = st.text_input('Servidor', value='ec2-18-210-23-246.compute-1.amazonaws.com')
            database = st.text_input('Base de datos', value='PortalIntegradoGS1BD')
            user = st.text_input('Usuario')
            password = st.text_input('Password', type='password')
            encrypt = st.checkbox('Encrypt', value=True)
            trust = st.checkbox('TrustServerCertificate', value=True)
            submitted = st.form_submit_button('Conectar')

            if submitted:
                try:
                    engine = get_engine_from_values(server, database, user, password, encrypt, trust)
                    st.session_state.authenticated = True
                    st.success('Conectado correctamente.')
                    st.rerun()
                except Exception as e:
                    st.session_state.authenticated = False
                    st.error(f'Error de conexión: {e}')

    if not st.session_state.authenticated:
        st.info('Conéctate a la base de datos en la barra lateral para comenzar.')
        st.stop()
    else:
        st.markdown(hide_sidebar_css, unsafe_allow_html=True)

# Título Principal
st.markdown('<p class="main-title">Publicación GS1 → EDI</p>', unsafe_allow_html=True)

# ==============================================
# Parámetros y Filtros de la UI
# ==============================================
if 'page' not in st.session_state:
    st.session_state.page = 1

with st.expander("Filtros de Búsqueda y Paginación", expanded=True):
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        page_size = st.selectbox('Resultados por página', [50, 100, 200, 500], index=3)
    with col2:
        plataformas = ['(todas)', 'EDI', 'AltaEmpresa', 'BajaEmpresa', 'AltaUsuario']
        plat_default = plataformas.index('EDI') if 'EDI' in plataformas else 0
        plataforma_sel = st.selectbox('Filtrar por Plataforma', plataformas, index=plat_default)
    with col3:
        st.info('Por defecto, la búsqueda se limita a los registros de los últimos 30 días.')

# Definir parámetros para la consulta
end_date = datetime.now().date() + timedelta(days=1)
start_date = (datetime.now().date() - timedelta(days=30))
plat_param = None if plataforma_sel == '(todas)' else plataforma_sel

# ==============================================
# Consultas a la Base de Datos
# ==============================================
SQL_COUNT = """
    SELECT COUNT(*) AS total
    FROM [dbo].[LegacyJobs] j
    WHERE j.FechaAlta >= :start AND j.FechaAlta < :end
      AND (:plat IS NULL OR j.Plataforma = :plat)
"""
SQL_PAGE = """
    SELECT j.Id, j.FechaAlta, j.Plataforma, j.Metodo,
           j.MotivoRechazo, j.IdEmpresa,
           e.CodEmpre, e.RazonSocial, e.CUIT
    FROM [dbo].[LegacyJobs] j
    LEFT JOIN [dbo].[Empresas] e ON e.IdEmpresa = j.IdEmpresa
    WHERE j.FechaAlta >= :start AND j.FechaAlta < :end
      AND (:plat IS NULL OR j.Plataforma = :plat)
    ORDER BY j.FechaAlta DESC
    OFFSET :off ROWS FETCH NEXT :psz ROWS ONLY;
"""

@st.cache_data(ttl=60) # Cache para evitar re-cargas innecesarias
def fetch_data(_engine, start, end, plat, offset_val, psz_val):
    with _engine.begin() as conn:
        total = conn.execute(text(SQL_COUNT), {"start": start, "end": end, "plat": plat}).scalar() or 0
        result = conn.execute(
            text(SQL_PAGE),
            {"start": start, "end": end, "plat": plat, "off": offset_val, "psz": psz_val}
        )
        rows = result.fetchall()
        cols = list(result.keys())
        return total, pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)

page = st.session_state.page
offset = max((page - 1) * page_size, 0)
total, df = fetch_data(engine, start_date, end_date, plat_param, offset, page_size)

# ==============================================
# Dashboard y Resumen de Estado
# ==============================================
st.subheader("Resumen de la Página Actual")

# Lógica de semáforo (detección de errores críticos)
critical_pattern = r'Error al dar de alta la empresa|Error en el alta de la empresa\.\s*-\s*Invalid argument|No existe la empresa, no se creo el usuario|No existe el usuario, no se creo el usuario'
crit_mask = df['MotivoRechazo'].astype(str).str.contains(critical_pattern, case=False, na=False)
crit_count = int(crit_mask.sum())
ok_count = len(df) - crit_count

c1, c2, c3 = st.columns([1, 1, 2])
with c1:
    st.markdown(f"""
    <div class="metric-card error-card">
        <div class="metric-card-title">ERRORES CRÍTICOS</div>
        <div class="metric-card-value error-text">🔴 {crit_count}</div>
    </div>
    """, unsafe_allow_html=True)

with c2:
    st.markdown(f"""
    <div class="metric-card ok-card">
        <div class="metric-card-title">OK</div>
        <div class="metric-card-value ok-text">🟢 {ok_count}</div>
    </div>
    """, unsafe_allow_html=True)

with c3:
    with st.container(border=True):
        m1, m2 = st.columns(2)
        m1.metric('Total de registros (últimos 30 días)', total)
        m2.metric('Página actual', f"{page} de {max((total + page_size - 1) // page_size, 1)}")
        m3, m4 = st.columns(2)
        m3.metric('Registros en esta página', len(df))
        m4.metric('Plataforma filtrada', plataforma_sel)

st.markdown("---")

# ==============================================
# Tabla de Resultados y Navegación
# ==============================================
st.subheader("Resultados")

# Filtros visuales para la tabla
filter_col1, filter_col2 = st.columns(2)
with filter_col1:
    show_only_crit = st.checkbox("Mostrar SOLO errores críticos (rojo)", value=False)
with filter_col2:
    show_only_ok = st.checkbox("Mostrar SOLO OK (verde)", value=False)

if show_only_crit and show_only_ok:
    display_df = df
elif show_only_crit:
    display_df = df[crit_mask]
elif show_only_ok:
    display_df = df[~crit_mask]
else:
    display_df = df

# Controles de navegación de página
total_pages = max((total + page_size - 1) // page_size, 1)
nav1, nav2, nav3 = st.columns([2, 2, 8])

with nav1:
    if st.button('⬅️ Anterior', disabled=(page <= 1), use_container_width=True):
        st.session_state.page = max(page - 1, 1)
        st.rerun()
with nav2:
    if st.button('Siguiente ➡️', disabled=(offset + page_size >= total), use_container_width=True):
        st.session_state.page = page + 1
        st.rerun()

# Tabla de datos
if not display_df.empty:
    display_df = display_df.rename(columns={"MotivoRechazo": "Respuesta"})
    display_df['FechaAlta'] = pd.to_datetime(display_df['FechaAlta']).dt.strftime('%Y-%m-%d %H:%M:%S')

    def highlight_respuesta(row):
        is_critical = crit_mask.loc[row.name] if row.name in crit_mask.index else False
        if is_critical:
            return ['background-color: #ff4d4f; color: white;'] * len(row)
        elif pd.notna(row['Respuesta']) and str(row['Respuesta']).strip():
            return ['background-color: #eaffea;'] * len(row)
        return [''] * len(row)

    st.dataframe(
        display_df[['Id', 'FechaAlta', 'Plataforma', 'CodEmpre', 'RazonSocial', 'CUIT', 'Respuesta']].style.apply(highlight_respuesta, axis=1),
        use_container_width=True,
        hide_index=True
    )
else:
    st.warning('No se encontraron resultados para los filtros seleccionados.')

st.markdown("---")

# ==============================================
# Detalle de XML por Selección
# ==============================================
if not display_df.empty:
    st.subheader("🔍 Visor de Parámetros (XML)")
    with st.container(border=True):
        left, right = st.columns([1, 2])
        with left:
            job_id = st.selectbox('Selecciona un Job ID para ver sus parámetros:', options=list(display_df['Id']))
            st.info("El XML correspondiente al Job ID seleccionado se mostrará a la derecha.")

        with right:
            if job_id:
                try:
                    SQL_XML = "SELECT CAST(Parametros AS NVARCHAR(MAX)) AS ParametrosXml FROM [dbo].[LegacyJobs] WHERE Id = :id;"
                    with engine.begin() as conn:
                        xml_text = conn.execute(text(SQL_XML), {"id": int(job_id)}).scalar_one_or_none()

                    if xml_text:
                        st.code(prettify_xml(xml_text), language='xml', line_numbers=True)
                    else:
                        st.info('Este Job no tiene parámetros XML para mostrar.')
                except Exception as e:
                    st.error(f'No se pudo obtener el XML: {e}')