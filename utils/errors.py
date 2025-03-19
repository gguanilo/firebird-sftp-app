class FirebirdConnectionError(Exception):
    """Excepción personalizada para errores de conexión a Firebird."""
    pass

class FirebirdQueryError(Exception):
    """Excepción personalizada para errores al ejecutar consultas en Firebird."""
    pass


class SQLiteConnectionError(Exception):
    """Excepción personalizada para errores de conexión a SQLite."""
    pass

class SQLiteQueryError(Exception):
    """Excepción personalizada para errores al ejecutar consultas en SQLite."""
    pass