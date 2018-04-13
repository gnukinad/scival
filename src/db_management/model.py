
class ids(object):

    def __init__(self, **kwargs):

        self.id = 0
        self.name = ''
        self.country = ''
        self.city = ''
        self.child_id = []
        self.scival_id = 0
        self.scopus_id = 0
        self.other_names = []
        self.countryCode = ''
        self.region = ''
        self.sector = ''
        self.parent_id = []

        self.allowed_keys = [x for x in dir(self) if '__' not in x]

        self.set_items(**kwargs)


    def to_dict(self):

        return dict(((x, getattr(self, x)) for x in self.allowed_keys if not callable(getattr(self,x))))


    def set_items(self, **kwargs):

        self.__dict__.update((k, v) for k, v in kwargs.items() if k in self.allowed_keys)
