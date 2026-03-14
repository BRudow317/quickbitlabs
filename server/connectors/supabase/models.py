class ConnectionString:

    host: str
    port: int
    url: str
    user: str
    password: str
    sid: str
    dsn: str
    token: str
    connection_string: str
    def __init__(self, connection_string: str):

        self.connection_string = connection_string