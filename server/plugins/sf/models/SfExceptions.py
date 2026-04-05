class SfException(Exception):
    """Base class for all exceptions raised by the Salesforce plugin."""
    func_name: str
    message: str
    def __init__(self, message):
        import inspect
        self.message = message
        func_name = "UnknownFunction"
        exc_init = inspect.currentframe()
        if exc_init is not None and exc_init.f_back is not None:
            func_name = exc_init.f_back.f_code.co_name
        self.func_name = func_name
        super().__init__(f"[{func_name}] {message}")

class SfWarning(Warning):
    """Warning raised by the Salesforce plugin."""
    func_name: str
    message: str
    def __init__(self, message):
        import inspect
        self.message = message
        func_name = "UnknownFunction"
        exc_init = inspect.currentframe()
        if exc_init is not None and exc_init.f_back is not None:
            func_name = exc_init.f_back.f_code.co_name
        self.func_name = func_name
        super().__init__(f"[{func_name}] {message}")
    