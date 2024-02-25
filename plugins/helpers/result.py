from abc import ABCMeta

class Result(metaclass=ABCMeta): 
    def is_ok(self) -> bool: return NotImplemented
    def unwrap(self): return NotImplemented

class Error(Result):
    def __init__(self, err):
        self._err = err

    def __repr__(self):
        return f"Error: {self._err}"

    def is_ok(self):
        return False

    def unwrap(self):
        raise Exception("Trying to unwrap an error.")

class Ok(Result):
    def __init__(self, val):
        self._val = val

    def __repr__(self):
        return f"Ok: {self._val}"

    def is_ok(self):
        return True

    def unwrap(self):
        return self._val