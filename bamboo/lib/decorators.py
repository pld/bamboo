class classproperty(property):
    """Declare properties for classes."""

    def __get__(self, cls, owner):
        return self.fget.__get__(None, owner)()
