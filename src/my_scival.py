from pprint import pprint
import warnings
from urllib import parse, request
import logging
import os
import json
import urllib.error
import urllib


class BaseSearch:

    def __init__(self):

        self.http_error = None

        self.logger = logging.getLogger(__name__)
        self.logger.debug("class was initialized")
        self.http_error = None
        self.response = None
        self.jres

    def encode(self):
        return self

    def send_request(self):

        response = None

        try:
            self.logger.debug("sending the request")
            response = request.urlopen(self.parsed_url)

            self.logger.debug("request retrieved sucessully")

        except urllib.error.HTTPError as e:

            if e.code == 404:
                self.logger.warning("page does not exist, error code is {}".format(e.code))
                self.logger.warning("error is {}".format(e))
            elif e.code == 400:
                self.logger.warning("invalid request, try again, error code is {}".format(e.code))
                self.logger.warning("error is {}".format(e))
            elif e.code == 401:
                self.logger.warning("cannot be authentified due to missing/invalid credentials")
                self.logger.warning("error is {}".format(e))
            elif e.code == 429:
                self.logger.warning("quota exceeded")
                self.logger.warning("error is {}".format(e))
            else:
                self.logger.warning("error code is {}, error is {}".format(e.code, e))

            self.http_error = e

        except Exception as e:

            response = None
            self.logger.warning("request retrieved with error, the error code is {}".format(e.code))
            self.logger.warning("error is {}".format(e))

        self.response = response

        return self


    def retrieve_json(self):
        """
        this function retrieves the json from the html response as a ready text for further analysis
        """
        
        if self.response is not None:
            output = json.loads(self.response.read())
            self.jres = output
        elif self.response is None:
            self.jres = None
        
        return self

    
    def get_jres(self):
        """
        this function gathers the whole pipline of getting the aff_id as json response
        """

        self.encode()
        self.send_request()
        self.retrieve_json()
        return self


class InstitutionSearch:

    def __str__(self):
        print("url is {}\nresponse is{}".format(self.parsed_url, self.jres))

    def __init__(self, query_type=None, universityName=None, apiKey=None, httpAccept=None, fname_log=None):

        if query_type is None:
            query_type="name"

        if universityName is None:
            universityName = "Harvard University"

        if apiKey is None:
            warnings.warn("apiKey is not defined")
            apiKey = "7f59af901d2d86f78a1fd60c1bf9426a"
            # apiKey = "e53785aedfc1c54942ba237f8ec0f891"


        if httpAccept is None:
            httpAccept = 'application/json'

        if fname_log is None:
            fname_log = os.path.join("logs", "my_scival.txt")

        if isinstance(universityName, str):
            universityName = [universityName]


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
        self.jres = ''


        # creating a logger
        self.logger = logging.getLogger(__name__)
        self.logger.debug("class was initialized")
        self.http_error = None


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

        response = None

        try:
            self.logger.debug("sending the request")
            response = request.urlopen(self.parsed_url)

            self.logger.debug("request retrieved sucessully")

        except urllib.error.HTTPError as e:

            if e.code == 404:
                self.logger.warning("page does not exist, error code is {}".format(e.code))
                self.logger.warning("error is {}".format(e))
            elif e.code == 400:
                self.logger.warning("invalid request, try again, error code is {}".format(e.code))
                self.logger.warning("error is {}".format(e))
            elif e.code == 401:
                self.logger.warning("cannot be authentified due to missing/invalid credentials")
                self.logger.warning("error is {}".format(e))
            elif e.code == 429:
                self.logger.warning("quota exceeded")
                self.logger.warning("error is {}".format(e))
            else:
                self.logger.warning("error code is {}, error is {}".format(e.code, e))

            self.http_error = e

        except Exception as e:

            response = None
            self.logger.warning("request retrieved with error, the error code is {}".format(e.code))
            self.logger.warning("error is {}".format(e))

        self.response = response

        return self



    def retrieve_json(self):
        """
        this function retrieves the json from the html response as a ready text for further analysis
        """
        
        if self.response is not None:
            output = json.loads(self.response.read())
            self.jres = output
        elif self.response is None:
            self.jres = None
        
        return self

    
    def get_jres(self):
        """
        this function gathers the whole pipline of getting the aff_id as json response
        """

        self.encode()
        self.send_request()
        self.retrieve_json()
        return self





class MetricSearch():

    def __init__(self, apiKey=None, aff_id=None, metrics=None, fname_log=None, path_to_save=None):

        """
        class to retrieve the metrics data for the unviersity from the scival

        inputs:
        apiKey - api key to get an access to the scival
        aff_id - affiliation id, the numieric id of the university or organization
        metrics - a string or a list of strings indicating which metrics will be downloaded from the scival, leaving it empty retrieves all of the metrics
        httpAccept - output data response, i.e. xml or json, Default is json
        fname_log - log filename


        """

        # for more infomration, please, have a look here
        # https://dev.elsevier.com/scival.html#!/SciVal_Metrics/getQueryMetrics


        # creating a logger
        self.logger = logging.getLogger(__name__)

        self.logger.debug("class was initialized")

        self.__all_httpAccept = ["application/json", "application/xml", "text/xml"]

        self.__all_metrics = ["Collaboration", "CitationCount", "CitationsPerPublication", "CollaborationImpact", "CitedPublications", "FieldWeightedCitationImpact", "hIndices", "ScholarlyOutput", "PublicationsInTopJournalPercentiles", "OutputsInTopCitationPercentiles"]

        self.__all_indexType = ["hIndex", "gIndex", "mIndex"] # not applicable to all metrics

        self.__url = "https://api.elsevier.com/metrics"

        self.__all_includedDocs = ["AllPublicationTypes", "ArticlesOnly", "ArticlesReviews", "ArticlesReviewsConferencePapers", "ArticlesReviewsEditorials", "ArticlesReviewsEditorialsShortSurveys", "ConferencePapersOnly", "ArticlesConferencePapers"]

        self.__all_yearRange = ["3yrs", "3yrsAndCurrent", "3yrsAndCurrentAndFuture", "5yrs", "5yrsAndCurrent", "5yrsAndCurrentAndFuture"]

        self.__all_journalImpactType = ["CiteScore", "SNIP", "SJR"]


        # default values for variables

        # a list of strings
        if aff_id is None:
            aff_id = ["508175"]
        else:
            if isinstance(aff_id, list):
                aff_id = [str(x) for x in aff_id]
            else:
                aff_id = [str(aff_id)]

        if apiKey is None:
            self.logger.warn("apiKey is not defined")
            apiKey = "7f59af901d2d86f78a1fd60c1bf9426a"
            # apiKey = "e53785aedfc1c54942ba237f8ec0f891"

        if fname_log is None:
            fname_log = os.path.join("logs", "my_scival.txt")

        if metrics is None:
            self.metrics = self.__all_metrics.copy()
            self.metrics.pop(self.metrics.index('hIndices'))   # this item cannot be used on several universities

        if path_to_save is None:
            path_to_save = os.path.join("data", "urlretrieve")


        self.aff_id = aff_id
        self.apiKey = apiKey

        self.parsed_data = ''
        self.parsed_url = ''

        self.fname_log = fname_log

        self.api_response = ''
        self.api_text = ''
        self.jres = ''
        self.http_error = None

        self.__yearRange = self.__all_yearRange[3]
        self.__httpAccept = self.__all_httpAccept[0]
        self.__indexType = self.__all_indexType[0]
        self.__includedDocs = self.__all_includedDocs[0]
        self.__journalImpactType = self.__all_journalImpactType[1]


        self.__all_string_bool = ["true", "false"]

        self.__includeSelfCitations = self.__all_string_bool[0]
        self.__byYear = self.__all_string_bool[0]
        self.__showAsFieldWeighted = self.__all_string_bool[1]



    def get_url(self):
        return self.__url

    def set_url(self, url):
        self.__url = url
        

    def check_index(self, a=None):

        if a is None:
            a = self.indexType

        if a in self.__all_index:
            self.indexType = a


    def check_httpAccept(self, a=None):
        """
        check whether new httpAccept is in the list of possible httpAccepts

        :param a: new httpAccept
        :return: nothing, but httpAccept is set to an accepted httpAccept
        """

        if a is None:
            a = self.httpAccept

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

 
        # xml
        # https://api.elsevier.com/metrics/?apiKey=e53785aedfc1c54942ba237f8ec0f891&httpAccept=application%2Fxml&metrics=Collaboration%2CCitationCount%2CCitationsPerPublication%2CCollaborationImpact%2CCitedPublications%2CFieldWeightedCitationImpact%2CScholarlyOutput%2CPublicationsInTopJournalPercentiles%2COutputsInTopCitationPercentiles&institutions=508175%2C508076&yearRange=5yrs&includeSelfCitations=true&byYear=true&includedDocs=AllPublicationTypes&journalImpactType=CiteScore&showAsFieldWeighted=false&indexType=hIndex

        # json
        # https://api.elsevier.com/metrics/?apiKey=e53785aedfc1c54942ba237f8ec0f891&httpAccept=application%2Fjson&metrics=Collaboration%2CCitationCount%2CCitationsPerPublication%2CCollaborationImpact%2CCitedPublications%2CFieldWeightedCitationImpact%2CScholarlyOutput%2CPublicationsInTopJournalPercentiles%2COutputsInTopCitationPercentiles&institutions=508175%2C508076&yearRange=5yrs&includeSelfCitations=true&byYear=true&includedDocs=AllPublicationTypes&journalImpactType=CiteScore&showAsFieldWeighted=false&indexType=hIndex

        # https://api.elsevier.com/metrics/?apiKey=7f59af901d2d86f78a1fd60c1bf9426a&httpAccept=application%2Fjson&metrics=Collaboration%2CCitationCount%2CCitationsPerPublication%2CCollaborationImpact%2CCitedPublications%2CFieldWeightedCitationImpact%2CScholarlyOutput%2CPublicationsInTopJournalPercentiles%2COutputsInTopCitationPercentiles&institutions=508175%2C508076&yearRange=5yrs&includeSelfCitations=true&byYear=true&includedDocs=AllPublicationTypes&journalImpactType=CiteScore&showAsFieldWeighted=false&indexType=hIndex

        query_metrics = ",".join(self.metrics)
        query_metrics = parse.quote(query_metrics)

        query_dict = {
                            'apiKey': self.apiKey,
                            'httpAccept': parse.quote_plus(self.__httpAccept),
                            'metrics': query_metrics,
                            'institutions': parse.quote(",".join(self.aff_id)),
                            'yearRange': self.__yearRange,
                            'includeSelfCitations': self.__includeSelfCitations,
                            'byYear': self.__byYear,
                            'includedDocs': self.__includedDocs,
                            'journalImpactType': self.__journalImpactType,
                            'showAsFieldWeighted': self.__showAsFieldWeighted,
                            'indexType': self.__indexType
                            }

        parsed_request = "&".join(["=".join([str(key), str(query_dict[key])]) for key in query_dict])

        # self.parsed_data = "{}&{}".format(parsed_request, query_metrics)
        self.parsed_data = parsed_request
        self.parsed_url = "{}/?{}".format(self.__url, self.parsed_data)
        self.logger.debug("encoding the request")

        return self


    def send_request(self):


        try:
            self.logger.debug("sending the request")
            response = request.urlopen(self.parsed_url)

            self.logger.debug("request retrieved sucessully")


        except urllib.error.HTTPError as e:

            if e.code == 404:
                self.logger.warning("page does not exist, error code is {}".format(e.code))
                self.logger.warning("error is {}".format(e))
            elif e.code == 400:
                self.logger.warning("invalid request, try again, error code is {}".format(e.code))
                self.logger.warning("error is {}".format(e))
            elif e.code == 401:
                self.logger.warning("cannot be authentified due to missing/invalid credentials")
                self.logger.warning("error is {}".format(e))
            elif e.code == 429:
                self.logger.warning("quota exceeded")
                self.logger.warning("error is {}".format(e))
            else:
                self.logger.warning("error code is {}, error is {}".format(e.code, e))

            self.http_error = e

        except Exception as e:

            response = e
            self.logger.warning("request retrieved with error, the error code is {}".format(e.code))
            self.logger.warning("error is {}".format(e))

        self.response = response

        return self


    def retrieve_json(self):
        """
        this function retrieves the json from the html response as a ready text for further analysis
        """
        
        try:
            output = json.loads(self.response.read())
        except:
            output = self.response

        self.jres = output
        
        return self


    def get_jres(self):
        """
        this function gathers the whole pipline of getting the aff_id as json response
        """

        self.encode()
        self.send_request()
        self.retrieve_json()
        return self
