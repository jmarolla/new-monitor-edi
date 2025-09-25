# app.py
import os, re, html, mimetypes, base64, json, time, io
from urllib.parse import urlencode

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from xml.dom import minidom
from datetime import datetime, timedelta
import streamlit.components.v1 as components  # HTML+JS en iframe
import requests

# ===================== PAGE / FAVICON =====================
FAVICON_PATH = "assets/favicon.png"
st.set_page_config(
    page_title="Publicación GS1 → EDI",
    page_icon=FAVICON_PATH if os.path.exists(FAVICON_PATH) else None,
    layout="wide",
)
if not os.path.exists(FAVICON_PATH):
    # Fallback para evitar el ícono default de Streamlit
    _PNG_DOT = ("iVBORw0KGgoAAAANSUhEUgAAAA4AAAAOCAYAAAAfSC3RAAAALElEQVQ4T2NkwA7+"
                "z0AEYQxgGJgYFQYwQ0gYg8gC4i1Q0A0Ew0gqA0kQAAH1kEKyBd8zMAAAAASUVORK5CYII=")
    st.markdown(f"""
    <script>(function(){{
      const l=document.querySelector("link[rel='icon']")||document.createElement('link');
      l.rel='icon'; l.type='image/png'; l.href='data:image/png;base64,{_PNG_DOT}';
      document.head.appendChild(l);
    }})();</script>
    """, unsafe_allow_html=True)

# ===================== TÍTULO (Logo + texto) =====================
def data_uri(path: str) -> str | None:
    try:
        mime = mimetypes.guess_type(path)[0] or "image/png"
        with open(path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("ascii")
        return f"data:{mime};base64,{b64}"
    except Exception:
        return None

LOGO_PATH = "assets/gs1-logo.png"
LOGO_HEIGHT = 64  # ajustá el tamaño del logo en px
logo_uri = data_uri(LOGO_PATH)

st.markdown(f"""
<div style="display:flex;align-items:center;justify-content:center;gap:14px;margin:8px 0 12px 0;">
  {f'<img src="{logo_uri}" alt="GS1" style="height:{LOGO_HEIGHT}px;object-fit:contain;">' if logo_uri else ''}
  <span style="font-size:36px;font-weight:800;">Publicación GS1 → EDI</span>
</div>
""", unsafe_allow_html=True)

# ===================== UTILS GENERALES =====================
def prettify_xml(xml_text: str) -> str:
    try:
        return minidom.parseString(xml_text.encode('utf-8')).toprettyxml(indent='  ')
    except Exception:
        return xml_text

def yesno(val, default_yes=True):
    s = str(val).lower()
    if s in ("1","true","yes","y","si","sí"): return "yes"
    if s in ("0","false","no"): return "no"
    return "yes" if default_yes else "no"

def esc(x):  # escape seguro para HTML
    return html.escape("" if x is None else str(x))

# ===================== MONITOR GS1→EDI =====================
def monitor_ui():
    @st.cache_resource(show_spinner=False)
    def get_engine(server, database, user, password, encrypt="yes", trust="yes"):
        url = (
            f"mssql+pytds://{user}:{password}@{server}:1433/{database}"
            f"?encrypt={yesno(encrypt)}&trustservercertificate={yesno(trust)}&autocommit=True"
        )
        eng = create_engine(url, pool_pre_ping=True, pool_recycle=180)
        with eng.begin() as c:
            c.execute(text("SELECT 1"))
        return eng

    def secrets_ok():
        try:
            s = st.secrets
            return all(k in s for k in ("DB_SERVER","DB_NAME","DB_USER","DB_PASS"))
        except Exception:
            return False

    # -------- Conexión (secrets o login manual) --------
    engine = None
    if secrets_ok():
        s = st.secrets
        try:
            engine = get_engine(
                s["DB_SERVER"], s["DB_NAME"], s["DB_USER"], s["DB_PASS"],
                s.get("DB_ENCRYPT","yes"), s.get("DB_TRUST","yes")
            )
            # Oculta la sidebar si usamos secrets
            st.markdown("<style>[data-testid='stSidebar'],[data-testid='collapsedControl']{display:none!important}</style>", unsafe_allow_html=True)
        except Exception as e:
            st.error(f"No se pudo conectar con secrets: {e}")
            return
    else:
        if 'auth' not in st.session_state: st.session_state.auth = False
        with st.sidebar:
            st.header("Login SQL Server")
            st.markdown("""
            <style>button[aria-label="Show password text"],button[aria-label="Hide password text"]{display:none!important}</style>
            """, unsafe_allow_html=True)
            with st.form("login", clear_on_submit=False):
                server   = st.text_input("Servidor")
                database = st.text_input("Base de datos")
                user     = st.text_input("Usuario")
                password = st.text_input("Password", type="password")
                encrypt  = st.checkbox("Encrypt=yes", value=True)
                trust    = st.checkbox("TrustServerCertificate=yes", value=True)
                ok = st.form_submit_button("Conectar")
        if ok:
            try:
                engine = get_engine(server, database, user, password, encrypt, trust)
                st.session_state.auth = True
                st.success("Conectado.")
            except Exception as e:
                st.error(f"Error de conexión: {e}")
        if not engine:
            st.info("Conéctate en la barra lateral para comenzar.")
            return
        else:
            st.markdown("<style>[data-testid='stSidebar'],[data-testid='collapsedControl']{display:none!important}</style>", unsafe_allow_html=True)

    # -------- Filtros --------
    if 'page' not in st.session_state: st.session_state.page = 1

    c1,c2,c3 = st.columns([1,1,2])
    with c1:
        page_size = st.selectbox("Filas por página", [50,100,200,500], index=1)
    with c2:
        plataformas = ['(todas)','EDI','AltaEmpresa','BajaEmpresa','AltaUsuario']
        plataforma  = st.selectbox("Plataforma", plataformas, index=plataformas.index('EDI') if 'EDI' in plataformas else 0)
    with c3:
        st.caption("Consulta limitada a los últimos 30 días (server-side).")

    end_date = datetime.now().date() + timedelta(days=1)
    start_date = datetime.now().date() - timedelta(days=30)
    plat_param = None if plataforma == '(todas)' else plataforma

    # -------- SQL --------
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

    with engine.begin() as conn:
        total = conn.execute(text(SQL_COUNT), {"start": start_date, "end": end_date, "plat": plat_param}).scalar() or 0

    with engine.begin() as conn:
        rs = conn.execute(text(SQL_PAGE), {"start": start_date, "end": end_date, "plat": plat_param, "off": offset, "psz": page_size})
        rows = rs.fetchall(); cols = list(rs.keys())
        df = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols or
             ["Id","FechaAlta","Plataforma","Metodo","MotivoRechazo","IdEmpresa","CodEmpre","RazonSocial","CUIT"])

    # -------- Semáforo --------
    critical_pattern = r'(Error al dar de alta la empresa|Error en el alta de la empresa\.\s*-\s*Invalid argument supplied for foreach\(\)|No existe la empresa, no se creo el usuario|No existe el usuario, no se creo el usuario)'
    crit_mask = df['MotivoRechazo'].astype(str).str.contains(critical_pattern, case=False, na=False)
    crit_count = int(crit_mask.sum())
    ok_count   = int(len(df) - crit_count)

    a,b = st.columns(2)
    a.markdown(f"""<div style="padding:20px;border-radius:16px;background:#ff4d4f;color:white;font-weight:900;
    font-size:28px;text-align:center;box-shadow:0 8px 20px rgba(255,77,79,.45);margin-bottom:14px;">🔴 ERROR: {crit_count}</div>""",
               unsafe_allow_html=True)
    b.markdown(f"""<div style="padding:20px;border-radius:16px;background:#06c1671a;color:#0e7a3f;font-weight:900;
    font-size:28px;text-align:center;border:3px solid #23c16b;box-shadow:0 8px 20px rgba(35,193,107,.35);margin-bottom:14px;">🟢 OK: {ok_count}</div>""",
               unsafe_allow_html=True)

    m1,m2,m3,m4 = st.columns(4)
    m1.metric('Total últimos 30 días', total); m2.metric('Página', page); m3.metric('Filas página', len(df)); m4.metric('Plataforma', plataforma)

    # -------- Checkboxes excluyentes --------
    if 'show_only_crit' not in st.session_state: st.session_state.show_only_crit = False
    if 'show_only_ok'   not in st.session_state: st.session_state.show_only_ok   = False
    if 'last_toggle'    not in st.session_state: st.session_state.last_toggle    = None  # 'crit'|'ok'|None

    if st.session_state.show_only_crit and st.session_state.show_only_ok:
        keep = st.session_state.last_toggle or 'crit'
        if keep == 'crit': st.session_state.show_only_ok = False
        else:              st.session_state.show_only_crit = False

    prev_crit = st.session_state.show_only_crit
    prev_ok   = st.session_state.show_only_ok

    c_crit, c_ok = st.columns(2)
    with c_crit:
        st.checkbox("Ver SOLO errores críticos (rojo)", key="show_only_crit", disabled=st.session_state.show_only_ok)
    with c_ok:
        st.checkbox("Ver SOLO OK (verde)", key="show_only_ok", disabled=st.session_state.show_only_crit)

    if st.session_state.show_only_crit and not prev_crit:
        st.session_state.last_toggle = 'crit'
    elif st.session_state.show_only_ok and not prev_ok:
        st.session_state.last_toggle = 'ok'
    elif (not st.session_state.show_only_crit and prev_crit) or (not st.session_state.show_only_ok and prev_ok):
        st.session_state.last_toggle = None

    # Dataset a mostrar
    if st.session_state.show_only_crit:
        display_df = df[crit_mask]
    elif st.session_state.show_only_ok:
        display_df = df[~crit_mask]
    else:
        display_df = df

    # -------- Navegación --------
    c1, c2, c3, c4 = st.columns([1, 1, 3, 3])

    if c1.button("⬅️ Anterior", disabled=(page <= 1)):
        st.session_state.page = max(page - 1, 1)
        st.rerun()

    if c2.button("Siguiente ➡️", disabled=(offset + page_size >= total)):
        st.session_state.page = page + 1
        st.rerun()

    goto = c3.number_input(
        "Ir a página",
        min_value=1,
        max_value=max((total + page_size - 1) // page_size, 1),
        value=page,
        step=1,
    )
    if c4.button("Ir"):
        st.session_state.page = int(goto)
        st.rerun()

    # -------- Tabla con botón Copiar (iframe) --------
    st.markdown("### Resultados")
    if not display_df.empty:
        df_show = display_df.rename(columns={"MotivoRechazo":"Respuestas"}).copy()
        df_show["FechaAlta"] = pd.to_datetime(df_show["FechaAlta"], errors="coerce")
        cols_show = ['Id','FechaAlta','Plataforma','CodEmpre','RazonSocial','CUIT','Respuestas']
        cols_show = [c for c in cols_show if c in df_show.columns]

        critical_re = re.compile(critical_pattern, re.IGNORECASE)
        def td_copy(val):
            return (
                f"<td class='copy-cell'>"
                f"<span class='cell-text'>{esc(val)}</span>"
                f"<button class='copybtn' data-text='{esc(val)}' title='Copiar'>📋</button>"
                f"</td>"
            )

        rows_html = []
        for _, r in df_show[cols_show].iterrows():
            row_class = "row-critical" if critical_re.search(str(r.get('Respuestas') or "")) else "row-ok"
            rows_html.append(
                f"<tr class='{row_class}'>"
                f"<td>{esc(r.get('Id'))}</td>"
                f"<td>{esc(r.get('FechaAlta'))}</td>"
                f"<td>{esc(r.get('Plataforma'))}</td>"
                f"{td_copy(r.get('CodEmpre'))}"
                f"<td>{esc(r.get('RazonSocial'))}</td>"
                f"{td_copy(r.get('CUIT'))}"
                f"{td_copy(r.get('Respuestas'))}"
                f"</tr>"
            )

        html_doc = f"""
        <!doctype html>
        <html>
        <head>
          <meta charset="utf-8"/>
          <style>
            body {{ margin:0; font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,"Helvetica Neue",Arial; }}
            .gs1tbl {{ width:100%; border-collapse:collapse; }}
            .gs1tbl th, .gs1tbl td {{ border:1px solid #e5e7eb; padding:8px; font-size:14px; vertical-align:top; }}
            .gs1tbl th {{ background:#f8fafc; text-align:left; position:sticky; top:0; z-index:1; }}
            .copy-cell {{ white-space:nowrap; }}
            .copybtn {{
              margin-left:8px; cursor:pointer; border:0; background:#eef2ff; color:#1f2937;
              border-radius:6px; padding:2px 6px; font-size:12px;
            }}
            .copybtn.ok {{ background:#d1fae5; }}
            .row-critical td {{ background:#ff4d4f; color:white; }}
            .row-ok td {{ background:#eaffea; color:black; }}
          </style>
        </head>
        <body>
          <table class="gs1tbl">
            <thead>
              <tr>
                <th>Id</th><th>FechaAlta</th><th>Plataforma</th>
                <th>CodEmpre</th><th>RazonSocial</th><th>CUIT</th><th>Respuestas</th>
              </tr>
            </thead>
            <tbody>
              {''.join(rows_html)}
            </tbody>
          </table>

          <script>
            (function(){{
              function copyFallback(text) {{
                const ta = document.createElement('textarea');
                ta.value = text; ta.style.position='fixed'; ta.style.opacity='0';
                document.body.appendChild(ta); ta.focus(); ta.select();
                try {{ document.execCommand('copy'); }} catch(e) {{}}
                document.body.removeChild(ta);
              }}
              document.addEventListener('click', function(ev){{
                const btn = ev.target.closest('.copybtn');
                if(!btn) return;
                const text = btn.getAttribute('data-text') || '';
                if (navigator.clipboard && navigator.clipboard.writeText) {{
                  navigator.clipboard.writeText(text).catch(function(){{ copyFallback(text); }});
                }} else {{
                  copyFallback(text);
                }}
                const old = btn.textContent; btn.textContent='✓'; btn.classList.add('ok');
                setTimeout(function(){{ btn.textContent=old; btn.classList.remove('ok'); }}, 900);
              }});
              function postHeight(){{
                const h = document.documentElement.scrollHeight || document.body.scrollHeight;
                parent.postMessage({{ stHeight: h }}, "*");
              }}
              setTimeout(postHeight, 50); setTimeout(postHeight, 200); setTimeout(postHeight, 600);
            }})();
          </script>
        </body>
        </html>
        """

        est_height = 420 + 22 * len(df_show)
        components.html(html_doc, height=min(max(est_height, 300), 2000), scrolling=True)
    else:
        st.warning("No hay resultados para los filtros actuales.")

    st.markdown("---")

    # -------- XML por Job --------
    if not display_df.empty:
        left,right = st.columns([1,2])
        with left:
            job_id = st.selectbox("Selecciona un Job Id para ver su XML de Parametros", options=list(display_df['Id']))
        SQL_XML = """SELECT CAST(Parametros AS NVARCHAR(MAX)) AS ParametrosXml
                     FROM [PortalIntegradoGS1BD].[dbo].[LegacyJobs] WHERE Id = :id"""
        xml_text = ''
        try:
            with engine.begin() as conn:
                row = conn.execute(text(SQL_XML), {"id": int(job_id)}).fetchone()
                if row and row[0]: xml_text = str(row[0])
        except Exception as e:
            st.error(f"No se pudo obtener el XML: {e}")
        with right:
            st.subheader("XML de Parametros")
            if xml_text:
                st.code(prettify_xml(xml_text), language="xml")
            else:
                st.info("Selecciona un Job con Parametros disponibles.")

# ===================== API TESTER =====================
def _kv_textarea_to_dict(txt: str) -> dict:
    """Convierte líneas 'Clave: Valor' en dict."""
    out = {}
    for line in (txt or "").splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        k, v = line.split(":", 1)
        out[k.strip()] = v.strip()
    return out

def _query_params_to_dict(txt: str) -> dict:
    """Convierte líneas 'key=value' en dict; soporta claves repetidas (lista)."""
    out = {}
    for line in (txt or "").splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip(); v = v.strip()
        if k in out:
            if isinstance(out[k], list):
                out[k].append(v)
            else:
                out[k] = [out[k], v]
        else:
            out[k] = v
    return out

def _pretty_json(text: str) -> str:
    try:
        obj = json.loads(text)
        return json.dumps(obj, indent=2, ensure_ascii=False)
    except Exception:
        return text

def api_tester_ui():
    st.subheader("API Tester")

    with st.form("api_form", clear_on_submit=False):
        m1, m2 = st.columns([1, 5])
        method = m1.selectbox("Método", ["GET", "POST", "PUT", "PATCH", "DELETE"], index=0)
        url = m2.text_input("URL", placeholder="https://api.ejemplo.com/endpoint")

        qcol, hcol = st.columns(2)
        with qcol:
            st.caption("Query params (key=value por línea)")
            qp_txt = st.text_area("Query params", value="", height=120, label_visibility="collapsed")
        with hcol:
            st.caption("Headers (Clave: Valor por línea)")
            headers_txt = st.text_area("Headers", value="", height=120, label_visibility="collapsed")

        b1, b2 = st.columns(2)
        with b1:
            auth_bearer = st.text_input("Bearer token (opcional)", type="password", placeholder="eyJhbGciOi...")
        with b2:
            content_type = st.selectbox("Content-Type (cuerpo)", ["application/json", "text/plain"], index=0)

        raw_body = st.text_area("Body (raw)", height=160, placeholder='{"key":"value"}  ó  texto plano')

        c1, c2, c3 = st.columns([1,1,2])
        timeout = c1.number_input("Timeout (s)", min_value=1, max_value=120, value=30, step=1)
        allow_redirects = c2.checkbox("Seguir redirects", value=True)
        send = c3.form_submit_button("Enviar", use_container_width=True)

    if not send:
        st.info("Complete los campos y haga clic en **Enviar**.")
        return

    params = _query_params_to_dict(qp_txt)
    headers = _kv_textarea_to_dict(headers_txt)
    if auth_bearer:
        headers["Authorization"] = f"Bearer {auth_bearer}"
    if content_type and method in ("POST", "PUT", "PATCH", "DELETE"):
        headers.setdefault("Content-Type", content_type)

    data = None
    json_data = None
    if method in ("POST", "PUT", "PATCH", "DELETE") and raw_body:
        if content_type == "application/json":
            try:
                json_data = json.loads(raw_body)
            except Exception as e:
                st.error(f"Body JSON inválido: {e}")
                return
        else:
            data = raw_body.encode("utf-8")

    sent_at = time.perf_counter()
    try:
        resp = requests.request(
            method=method,
            url=url,
            params=params,
            headers=headers,
            data=data,
            json=json_data,
            timeout=timeout,
            allow_redirects=allow_redirects,
        )
        elapsed = (time.perf_counter() - sent_at) * 1000.0
    except requests.exceptions.RequestException as e:
        st.error(f"Error al llamar a la API: {e}")
        return

    s1, s2, s3 = st.columns(3)
    s1.metric("Status", f"{resp.status_code}")
    s2.metric("Tiempo", f"{elapsed:.1f} ms")
    s3.metric("Tamaño", f"{len(resp.content)} bytes")

    with st.expander("Headers de respuesta", expanded=False):
        st.json(dict(resp.headers))

    ctype = resp.headers.get("Content-Type", "").lower()
    if "application/json" in ctype:
        st.code(_pretty_json(resp.text), language="json")
    elif any(x in ctype for x in ["text/plain", "text/html", "text/xml", "application/xml"]):
        st.code(resp.text)
    else:
        st.info(f"Contenido no textual ({ctype or 'desconocido'}). Puede descargarlo abajo.")
        st.download_button(
            "Descargar contenido",
            data=io.BytesIO(resp.content),
            file_name="response.bin",
            mime=ctype or "application/octet-stream",
        )

    # cURL equivalente
    curl_parts = [f"curl -X {method}"]
    for k, v in headers.items():
        curl_parts.append(f"-H {json.dumps(f'{k}: {v}')}")
    if params:
        qs = urlencode(params, doseq=True)
        full_url = url + ("&" if "?" in url else "?") + qs
    else:
        full_url = url
    if json_data is not None:
        curl_parts.append(f"--data {json.dumps(json.dumps(json_data))}")
    elif data is not None and len(data) > 0:
        curl_parts.append(f"--data {json.dumps(data.decode('utf-8'))}")
    curl_parts.append(json.dumps(full_url))
    st.caption("cURL")
    st.code(" \\\n  ".join(curl_parts), language="bash")

# ===================== TABS PRINCIPALES =====================
tab_monitor, tab_api = st.tabs(["📊 Monitor GS1→EDI", "🧪 API Tester"])

with tab_monitor:
    monitor_ui()

with tab_api:
    api_tester_ui()
