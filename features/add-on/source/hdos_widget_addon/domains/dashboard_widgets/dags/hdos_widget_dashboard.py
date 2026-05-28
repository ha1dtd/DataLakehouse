"""Airflow DAG entrypoint placeholder for the HDOS widget add-on."""


ADDON_ID = "hdos-widget-addon"
DOMAIN_ID = "dashboard_widgets"
DAG_ID = "hdos_widget_dashboard"


def build_dag():
    """Reserved for the real widget add-on DAG implementation."""
    raise NotImplementedError("Implement the widget add-on DAG using the Lakehouse add-on runtime contract.")
