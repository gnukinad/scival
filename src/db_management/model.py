


mfwci = "FieldWeightedCitationImpact"
mcol_impact = "CollaborationImpact"
mcol = "Collaboration"
mcit_pubs = "CitedPublications"
mcit_per_pub = "CitationsPerPublication"
mcit = "CitationCount"
mscholar_output = "ScholarlyOutput"
mtjp = "PublicationsInTopJournalPercentiles"   # tjp - top journal percentiles
mocp = "OutputsInTopCitationPercentiles"       # ocp - outputs in citation percentiles
mbc = "BookCount"
mpc = "PatentCount"


metrics = [mfwci, mcol, mcol_impact, mcit, mcit_pubs, mcit_per_pub, mscholar_output, mtjp, mocp, mbc, mpc]


class base_model:

    def __init__(self):

        pass


    def is_member(self, field):

        return (not callable(getattr(self, field))) and getattr(self, field) and ('__' not in field)

    def to_dict(self):

        return dict((x, getattr(self, x)) for x in dir(self) if self.is_member(x))

    def set_items(self, **kwargs):

        self.__dict__.update((k, v) for k, v in kwargs.items() if (k in dir(self)) and (not callable(getattr(self, k))) and (v))



class ids(base_model):

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

        self.set_items(**kwargs)


class scopus_metrics(base_model):

    def __init__(self, **kwargs):

        self.id = 0
        self.scopus_id = 0
        self.scival_id = 0
        self.year = 0
        self.name = ''
        self.value = 0
        self.metricType = '' # either book_count or patent_count

        self.set_items(**kwargs)

        assert self.metricType == 'BookCount' or self.metricType == 'PatentCount', 'no such metric is allowed'



# class to acknowledge whether the metric was retrieved or not
class metric_ack_model(base_model):

    def __init__(self, **kwargs):

        self.id = 0
        self.scopus_id = 0
        self.scival_id = 0
        self.year = 0
        self.name = ''
        self.metricType = ''
        self.ack = 0

        self.set_items(**kwargs)

        assert self.metricType == 'BookCount' or self.metricType == 'PatentCount', 'no such metric is allowed'
