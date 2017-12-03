from pprint import pprint
import warnings
from urllib import parse, request
import logging
import os
import json


class Scival:

    def __init__(self):
        self.id = 0


    def req(self):

        pprint("i am in req, my id is ".format(self.id))


class InstitutionSearch:

    def __init__(self, query_type=None, universityName=None, apiKey=None, httpAccept=None, fname_log=None):

        if query_type is None:
            query_type="name"

        if universityName is None:
            universityName = "Harvard University"

        if apiKey is None:
            warnings.warn("apiKey is not defined")
            # apiKey = "7f59af901d2d86f78a1fd60c1bf9426a"
            apiKey = "e53785aedfc1c54942ba237f8ec0f891"


        if httpAccept is None:
            httpAccept = 'application/json'

        if fname_log is None:
            fname_log = os.path.join("logs", "my_scival.txt")


        self.__url = "https://api.elsevier.com/metrics/institution/search"

        self.query = universityName
        self.query_type = query_type
        self.apiKey = apiKey
        self.httpAccept = httpAccept

        self.__all_httpAccepts = ["application/json", "application/xml", "text/xml"]

        self.parsed_data = ''
        self.parsed_url = ''

        self.fname_log = fname_log

        self.api_response = ''
        self.api_text = ''


        # creating a logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)


        # create file handler which logs info messages
        fh = logging.FileHandler('foo.log', 'w', 'utf-8')
        fh.setLevel(logging.INFO)

        # create console handler with a debug log level
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # creating a formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)-8s: %(message)s')

        # setting handler format
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        # add the handlers to the logger
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

        self.logger.debug("class was initialized")


    def get_url(self):
        return self.__url

    def set_url(self, url):
        self.__url = url


    def check_httpAccept(self, a):
        """
        check whether new httpAccept is in the list of possible httpAccepts

        :param a: new httpAccept
        :return: nothing, but httpAccept is set to an accepted httpAccept
        """

        if a in self.__all_httpAccepts:
            self.httpAccept = a


    def encode(self, universityName=None, apiKey=None, httpAccept=None):

        if universityName is not None:
            self.query = universityName

        if apiKey is not None:
            self.apiKey = apiKey

        if httpAccept is not None:
            self.check_httpAccept(httpAccept)

        parse_query = "?query=" + "or".join("{}({})".format(self.query_type, parse.quote(x)) for x in self.query) + "&"

        parsed_request = parse.urlencode({
                            'apiKey': self.apiKey,
                            'httpAccept': self.httpAccept})

        self.parsed_data = "{}{}".format(parse_query, parsed_request)
        self.parsed_url = "{}{}".format(self.__url, self.parsed_data)
        self.logger.debug("encoding the request")
        return self


    def send_request(self):

        try:
            self.logger.debug("sending the request")
            response = request.urlopen(self.parsed_url)

            self.logger.debug("request retrieved sucessully")
        except Exception as e:

            response = e
            self.logger.warning("request retrieved with error, the error code is {}".format(e.code))
            self.logger.warning("error is {}".format(e))

        self.response = response

        return self


    def retrieve_json(response):
        """
        this function retrieves the json from the html response
        """
        
        output = json.loads(response.read())
        
        return output



class MetricSearch:

    # def __init__(self, aff_id):
    def __init__(self, apiKey=None, aff_id=None, metrics=None, httpAccept=None, fname_log=None, indexType=None):



        # creating a logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        # create file handler which logs info messages
        fh = logging.FileHandler('logs.txt', 'w', 'utf-8')
        fh.setLevel(logging.INFO)

        # create console handler with a debug log level
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # creating a formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)-8s: %(message)s')

        # setting handler format
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        # add the handlers to the logger
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

        self.logger.debug("class was initialized")

        # default values for variables
        if aff_id is None:
            aff_id = 508175

        if apiKey is None:
            self.logger.warn("apiKey is not defined")
            # apiKey = "7f59af901d2d86f78a1fd60c1bf9426a"
            apiKey = "e53785aedfc1c54942ba237f8ec0f891"

        if httpAccept is None:
            httpAccept = 'application/json'

        if fname_log is None:
            fname_log = os.path.join("logs", "my_scival.txt")

        if metrics is None:
            metrics = ["Collaboration", "CitationCount", "CitationPerPublication", "CollaborationImpact", "CitedPublications", "FieldWeightedCitationImpact", "hIndices", "ScholarlyOutput", "PublicationsInTopJournalPercentiles", "OutputsInTopCitationPercentiles"]

        if indexType is None:

            # https://api.elsevier.com/metrics/?apiKey=7f59af901d2d86f78a1fd60c1bf9426a&httpAccept=application%2Fjson&metrics=Collaboration%2CCitationCount%2CCitationPerPublication%2CCollaborationImpact%2CCitedPublications%2CFieldWeightedCitationImpact%2CHIndices%2CScholarlyOutput%2CPublicationsInTopJournalPercentiles%2COutputsInTopCitationPercentiles&institutions=508175%2C508076&yearRange=5yrs&includeSelfCitations=true&byYear=true&includedDocs=AllPublicationTypes&journalImpactType=CiteScore&showAsFieldWeighted=false&indexType=hIndex

        self.__url = "https://api.elsevier.com/metrics"

        self.aff_id = aff_id
        self.apiKey = apiKey
        self.httpAccept = httpAccept

        self.__all_httpAccepts = ["application/json", "application/xml", "text/xml"]

        self.__all_metrics = ["Collaboration", "CitationCount", "CitationPerPublication", "CollaborationImpact", "CitedPublications", "FieldWeightedCitationImpact", "hIndices", "ScholarlyOutput", "PublicationsInTopJournalPercentiles", "OutputsInTopCitationPercentiles"]

        self.parsed_data = ''
        self.parsed_url = ''

        self.fname_log = fname_log

        self.api_response = ''
        self.api_text = ''


    def get_url(self):
        return self.__url

    def set_url(self, url):
        self.__url = url


    def check_httpAccept(self, a):
        """
        check whether new httpAccept is in the list of possible httpAccepts

        :param a: new httpAccept
        :return: nothing, but httpAccept is set to an accepted httpAccept
        """

        if a in self.__all_httpAccepts:
            self.httpAccept = a


    def check_metrics(self, a):
        """
        check whether a metric in the list of possible metrics

        :param a: new httpAccept
        :return: nothing, but httpAccept is set to an accepted httpAccept
        """

        if all([x in self.__all_metrics for x in a]):
            self.metrics = a
        else:
            self.logger.warning("metric a is not in the list of possible metrics, trying all metrics")
            self.metrics = self.__all_metrics.copy()


    def encode(self, aff_id=None, metrics=None, apiKey=None, httpAccept=None):

        if aff_id is not None:
            self.aff_id = aff_id

        if apiKey is not None:
            self.apiKey = apiKey

        if httpAccept is not None:
            self.check_httpAccept(httpAccept)

        if metrics is not None:
            self.check_metrics(metrics)

        # https://api.elsevier.com/metrics/?apiKey=7f59af901d2d86f78a1fd60c1bf9426a&httpAccept=application%2Fjson&metrics=Collaboration%2CCitationCount%2CCitationPerPublication%2CCollaborationImpact%2CCitedPublications%2CFieldWeightedCitationImpact%2CHIndices%2CScholarlyOutput%2CPublicationsInTopJournalPercentiles%2COutputsInTopCitationPercentiles&institutions=508175%2C508076&yearRange=5yrs&includeSelfCitations=true&byYear=true&includedDocs=AllPublicationTypes&journalImpactType=CiteScore&showAsFieldWeighted=false&indexType=hIndex
        query_metrics = ",".join(self.metrics)
        query_metrics = "metrics=" + parse.quote(query_metrics)

        parsed_request = parse.urlencode({
                            'apiKey': self.apiKey,
                            'httpAccept': self.httpAccept})

        self.parsed_data = "&".join(parsed_request, query_metrics)
        self.parsed_url = "{}{}".format(self.__url, self.parsed_data.decode())
        self.logger.debug("encoding the request")
        return self


    def send_request(self):

        try:
            self.logger.debug("sending the request")
            response = request.urlopen(self.__url, self.parsed_data)

            self.logger.debug("request retrieved sucessully")
        except Exception as e:

            response = e
            self.logger.warning("request retrieved with error, the error code is {}".format(e.code))
            self.logger.warning("error is {}".format(e))

        self.response = response

        return response
