class T(object):
    single = None
    def __new__(cls,  *args, **kwargs):
        print("new")
        if cls.single is None:
            cls.single = super().__new__(cls)
        return cls.single
    def __init__(self,name):
        self.name = name
        self.get()
    def get(self):
        print(self.name)
t = T("name")
b = T("lisi")

