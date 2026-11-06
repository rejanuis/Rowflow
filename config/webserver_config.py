# Read by the FAB auth manager (AIRFLOW__FAB__CONFIG_FILE). Local DB users with roles Admin / Op / User / Viewer.
from flask_appbuilder.const import AUTH_DB

AUTH_TYPE = AUTH_DB
AUTH_USER_REGISTRATION = False          # nobody can self-register

SESSION_COOKIE_SECURE = True            # cookie only over HTTPS (nginx terminates TLS)
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"
WTF_CSRF_ENABLED = True

# --- SSO (recommended once you have an IdP): replace AUTH_DB with OAuth, e.g. Okta / Google / Azure ---
# from flask_appbuilder.const import AUTH_OAUTH
# AUTH_TYPE = AUTH_OAUTH
# OAUTH_PROVIDERS = [...]               # see the FAB provider "Single Sign-On (SSO)" docs
# AUTH_ROLES_MAPPING = {"airflow-admin-group": ["Admin"], "airflow-viewer-group": ["Viewer"]}
# AUTH_ROLES_SYNC_AT_LOGIN = True
# AUTH_USER_REGISTRATION_ROLE = "Viewer"
