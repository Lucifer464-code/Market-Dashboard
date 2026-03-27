import base64
import hashlib
import hmac
import json
import struct
import time
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

_logo_path = Path(__file__).parent.parent / "assets" / "logo.png"
_logo_b64 = base64.b64encode(_logo_path.read_bytes()).decode() if _logo_path.exists() else ""

AUTH_DURATION = 4 * 3600  # 4 hours


def _make_token(password: str) -> str:
    """Create an HMAC-signed token encoding the expiry timestamp."""
    expiry = int(time.time()) + AUTH_DURATION
    msg = struct.pack(">Q", expiry)
    sig = hmac.new(password.encode(), msg, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(msg + sig).decode().rstrip("=")


def _is_valid_token(token: str, password: str) -> bool:
    """Verify the token signature and check it hasn't expired."""
    try:
        padded = token + "=" * (-len(token) % 4)
        data = base64.urlsafe_b64decode(padded)
        msg, sig = data[:8], data[8:]
        expected = hmac.new(password.encode(), msg, hashlib.sha256).digest()
        if not hmac.compare_digest(sig, expected):
            return False
        expiry = struct.unpack(">Q", msg)[0]
        return time.time() < expiry
    except Exception:
        return False


def login_wall() -> bool:
    """Show login screen and return True once authenticated."""
    password = st.secrets["PASSWORD"]

    # On load: read token from localStorage and inject into query params if missing
    components.html("""
<script>
(function() {
  var tok = localStorage.getItem('ifpl_auth');
  if (!tok) return;
  var url = new URL(window.parent.location.href);
  if (!url.searchParams.get('auth_token')) {
    url.searchParams.set('auth_token', tok);
    window.parent.location.replace(url.toString());
  }
})();
</script>
""", height=0)

    token = st.query_params.get("auth_token")
    if token and _is_valid_token(token, password):
        st.session_state.authenticated = True
        return True

    if st.session_state.get("authenticated"):
        return True

    _, col, _ = st.columns([1, 1.4, 1])
    with col:
        st.markdown("<br><br><br>", unsafe_allow_html=True)
        if _logo_b64:
            st.markdown(
                f"<div style='text-align:center;margin-bottom:12px'>"
                f"<img src='data:image/png;base64,{_logo_b64}' width='56' height='56' "
                f"style='border-radius:12px;object-fit:cover' /></div>",
                unsafe_allow_html=True,
            )
        st.markdown(
            "<h2 style='text-align:center;font-size:22px;font-weight:700;color:#0f172a;margin-bottom:4px'>IFPL Market Dashboard</h2>",
            unsafe_allow_html=True,
        )
        st.markdown(
            "<p style='text-align:center;color:#94a3b8;font-size:13px;margin-bottom:24px'>Enter password to continue</p>",
            unsafe_allow_html=True,
        )
        with st.form("login_form"):
            pw = st.text_input(
                "Password",
                type="password",
                placeholder="Password",
                label_visibility="collapsed",
            )
            submitted = st.form_submit_button("Login", use_container_width=True, type="primary")
        if submitted:
            if pw == password:
                token = _make_token(password)
                st.query_params["auth_token"] = token
                st.session_state.authenticated = True
                # Persist token in localStorage so mobile browsers remember it
                components.html(f"""
<script>
localStorage.setItem('ifpl_auth', {json.dumps(token)});
window.parent.location.reload();
</script>
""", height=0)
                st.rerun()
            else:
                st.error("Incorrect password")

    return False
