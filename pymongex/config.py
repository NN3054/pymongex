class Config:
    connection_string = "SET MY MONGODB CONNECTION STRING"


def set_connection_string(conn_str: str):
    Config.connection_string = conn_str


def get_connection_string():
    return Config.connection_string
