class GetDashboardError(Exception):
    def __init__(self, dashboard_id, error):
        super().__init__(f"Could not get dashboard with id {dashboard_id}", error)
