"""
WR-Dev Site Selection — navigation SKELETON / mockup.

This is a clickable layout prototype, NOT the real tool. It demonstrates the
proposed three-section structure so the big picture can be reviewed before any
real data work happens:

    Landing page (3 section cards)
        └─▶ Section view: persistent stepper  +  exec/analyst toggle  +  content

Run it standalone (does not touch the working Land Screener in app.py):
    streamlit run src/app_shell.py

Everything below is placeholder content. Real data + the existing Land Screener
get folded in later, once this layout is approved.
"""

from pathlib import Path

import streamlit as st

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="WR-Dev Site Selection",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── WR-Dev logo (top-left) ───────────────────────────────────────────────────────
_LOGO_PATH = Path(__file__).parent.parent / "assets" / "wr_dev_logo.png"
if _LOGO_PATH.exists():
    st.logo(str(_LOGO_PATH), size="large")

# ── WR-Dev brand CSS (matches the real app) ─────────────────────────────────────
st.markdown("""
<style>
html, body, [class*="css"], button, input, select, textarea {
    font-family: Arial, sans-serif !important;
}
:root {
    --wr-teal:     #779FA1;
    --wr-gray:     #A1ABAC;
    --wr-warm:     #C5C5B9;
    --wr-dark:     #2c3e3f;
    --wr-light-bg: #f5f6f4;
}
.stApp { background-color: #ffffff; }

/* Enlarge the top-left logo beyond Streamlit's "large" preset */
img.stLogo, [data-testid="stHeaderLogo"] {
    height: 64px !important;
    width: auto !important;
    margin-top: 1.5rem !important;
    margin-left: 1.5rem !important;
}
/* Give the header room so the lowered logo doesn't clip */
[data-testid="stHeader"] { height: auto !important; }

h1 { color: var(--wr-teal) !important; }
h2, h3 { color: var(--wr-teal) !important; }

/* Section cards on the landing page */
.section-card {
    background: var(--wr-light-bg);
    border: 1px solid var(--wr-warm);
    border-left: 5px solid var(--wr-teal);
    border-radius: 10px;
    padding: 22px 22px 12px 22px;
    min-height: 190px;
}
.section-card .num {
    color: var(--wr-teal);
    font-size: 34px;
    font-weight: 700;
    line-height: 1;
}
.section-card h3 { margin: 6px 0 8px 0; }
.section-card p { color: var(--wr-dark); font-size: 14px; }

/* Stepper */
.stepper {
    display: flex; align-items: center; gap: 8px;
    margin: 4px 0 18px 0; font-size: 15px; font-weight: 600;
}
.step       { color: var(--wr-gray); }
.step.active { color: var(--wr-teal); }
.step.done  { color: var(--wr-dark); }
.step .dot {
    display: inline-block; width: 12px; height: 12px; border-radius: 50%;
    background: var(--wr-warm); margin-right: 6px; vertical-align: middle;
}
.step.active .dot { background: var(--wr-teal); }
.step.done  .dot { background: var(--wr-dark); }
.step-arrow { color: var(--wr-warm); }

/* Placeholder content boxes */
.placeholder {
    background: var(--wr-light-bg);
    border: 1px dashed var(--wr-gray);
    border-radius: 8px;
    padding: 28px;
    text-align: center;
    color: var(--wr-gray);
}
</style>
""", unsafe_allow_html=True)

# ── Navigation state ─────────────────────────────────────────────────────────────
# "home" = landing page; otherwise one of the three section keys.
if "section" not in st.session_state:
    st.session_state.section = "home"
if "submarket" not in st.session_state:
    st.session_state.submarket = None        # carry-forward demo: Market → Land
if "parcel" not in st.session_state:
    st.session_state.parcel = None            # carry-forward demo: Land → Financial

SECTIONS = [
    {"key": "market",    "num": "1", "title": "Market Feasibility",
     "blurb": "Where should we build? County housing needs, demographics, "
              "affordability, and a map of competing developments."},
    {"key": "land",      "num": "2", "title": "Land Screener",
     "blurb": "Which parcels? Zoning, floodplain, wetlands, and a 0–100 "
              "feasibility score per vacant parcel. (Your existing tool.)"},
    {"key": "financial", "num": "3", "title": "Financial Review",
     "blurb": "How much do we offer? Automated underwriting and a recommended "
              "land-pricing strategy for a chosen parcel."},
]
SECTION_KEYS = [s["key"] for s in SECTIONS]


def go(section_key: str):
    st.session_state.section = section_key


# ── Landing page ─────────────────────────────────────────────────────────────────
def render_home():
    st.title("WR-Dev Site Selection")
    st.caption("From market to parcel to offer — one connected workflow.")
    st.write("")

    cols = st.columns(3, gap="large")
    for col, s in zip(cols, SECTIONS):
        with col:
            st.markdown(
                f"""
                <div class="section-card">
                    <div class="num">{s['num']}</div>
                    <h3>{s['title']}</h3>
                    <p>{s['blurb']}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.button(f"Open {s['title']} →", key=f"open_{s['key']}",
                      use_container_width=True, on_click=go, args=(s["key"],))

    st.write("")
    st.markdown(
        '<p style="color:var(--wr-gray); font-size:14px; margin-top:8px;">'
        'Each section flows into the next — your market pick filters the land '
        'search, and your parcel pick pre-loads the financials.</p>',
        unsafe_allow_html=True,
    )


# ── Stepper (persistent inside any section) ──────────────────────────────────────
def render_stepper(current_key: str):
    current_idx = SECTION_KEYS.index(current_key)
    parts = []
    for i, s in enumerate(SECTIONS):
        cls = "active" if i == current_idx else ("done" if i < current_idx else "")
        parts.append(f'<span class="step {cls}"><span class="dot"></span>'
                     f'{s["num"]}. {s["title"]}</span>')
        if i < len(SECTIONS) - 1:
            parts.append('<span class="step-arrow">▸</span>')
    st.markdown(f'<div class="stepper">{"".join(parts)}</div>',
                unsafe_allow_html=True)

    # Quick-jump + home buttons
    nav = st.columns([1, 1, 1, 3])
    for col, s in zip(nav[:3], SECTIONS):
        col.button(s["title"], key=f"jump_{s['key']}",
                   use_container_width=True, on_click=go, args=(s["key"],))
    nav[3].button("⌂ Back to home", key="back_home",
                  use_container_width=True, on_click=go, args=("home",))
    st.divider()


# ── Per-section placeholder bodies ───────────────────────────────────────────────
def view_toggle(key: str) -> str:
    return st.radio("View", ["Executive", "Analyst"], horizontal=True,
                    key=f"view_{key}", label_visibility="collapsed")


def render_market():
    st.subheader("1. Market Feasibility")
    view = view_toggle("market")

    # Carry-forward demo: choosing a submarket here flows into Land
    submarket = st.selectbox(
        "Submarket (city)", ["Grand Haven", "Holland", "Muskegon"],
        index=0, key="market_submarket",
    )
    st.session_state.submarket = submarket

    if view == "Executive":
        c1, c2, c3 = st.columns(3)
        c1.metric("Median HH income", "$ —", help="placeholder")
        c2.metric("Max affordable rent (30%)", "$ —", help="income ÷ 12 × 30%")
        c3.metric("Competing projects", "—")
        st.markdown('<div class="placeholder">🗺️ Submarket demand heatmap + '
                    'competitor pins<br><small>(map placeholder)</small></div>',
                    unsafe_allow_html=True)
    else:
        st.markdown('<div class="placeholder">📊 Analyst tables: ACS demographics, '
                    'unit-gap by income band, full competitor list with sources, '
                    'score components<br><small>(tables placeholder)</small></div>',
                    unsafe_allow_html=True)

    st.success(f"Selected submarket **{submarket}** will carry into the Land "
               f"Screener.", icon="🔗")
    st.button("Continue to Land Screener →", on_click=go, args=("land",),
              type="primary")


def render_land():
    st.subheader("2. Land Screener")
    sm = st.session_state.submarket
    if sm:
        st.caption(f"🔗 Filtered to submarket from Market Feasibility: **{sm}**")
    view_toggle("land")
    st.markdown('<div class="placeholder">🏗️ Your existing Land Screener '
                '(map + scored parcels + filters) gets embedded here.<br>'
                '<small>Nothing is rebuilt — the working tool moves in as-is.</small>'
                '</div>', unsafe_allow_html=True)

    # Carry-forward demo: pick a parcel → flows into Financial
    parcel = st.selectbox("Pick a parcel (demo)",
                          ["— none —", "Parcel A (4.2 ac)", "Parcel B (6.8 ac)"],
                          key="land_parcel")
    st.session_state.parcel = None if parcel == "— none —" else parcel
    if st.session_state.parcel:
        st.success(f"Selected **{st.session_state.parcel}** will carry into "
                   f"Financial Review.", icon="🔗")

    st.button("Continue to Financial Review →", on_click=go,
              args=("financial",), type="primary")


def render_financial():
    st.subheader("3. Financial Review")
    p = st.session_state.parcel
    if p:
        st.caption(f"🔗 Pre-loaded parcel from Land Screener: **{p}**")
    else:
        st.caption("No parcel selected yet — pick one in the Land Screener.")
    view_toggle("financial")
    st.markdown('<div class="placeholder">💵 Underwriting pro forma + recommended '
                'land-offer range<br><small>(deal-sheet placeholder)</small></div>',
                unsafe_allow_html=True)


# ── Router ───────────────────────────────────────────────────────────────────────
RENDERERS = {
    "market":    render_market,
    "land":      render_land,
    "financial": render_financial,
}

if st.session_state.section == "home":
    render_home()
else:
    render_stepper(st.session_state.section)
    RENDERERS[st.session_state.section]()
