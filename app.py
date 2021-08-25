# Initial Config
from apps import app_layout, app_callbacks
from __init__ import app

# Initiate the app
server = app.server
print("Initiating app")

# Initiate the app layout
print("Initiating the app layout")
app.layout = app_layout.initiate_app_layout()

# Callbacks
print("Initiating the app callbacks")
app_callbacks.register_app_callbacks(app=app)

# Run the app
print("Running the app")
if __name__ == "__main__":
    app.run_server(debug=True)
