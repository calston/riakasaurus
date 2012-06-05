from xml.etree import ElementTree
from xml.dom.minidom import Document

from riakasaurus import util


class RiakSearch(object):
    def __init__(self, client, transport_class=None,
                 host="127.0.0.1", port=8098):
        self._client = client
        self._host = host
        self._port = port
        self._decoders = {"text/xml": ElementTree.fromstring}

    def get_decoder(self, content_type):
        decoder = self._client.get_decoder(content_type) or self._decoders.get(content_type)
        if not decoder:
            decoder = self.decode

        return decoder

    def decode(self, data):
        return data

    def _do_xml_request(self, url, xml):
        headers = {'Accept': 'text/xml, */*; q=0.5',
                   'Content-Type': 'text/xml',
                   }
        d = util.http_request_deferred('POST', self._host, self._port, url,
                                       headers, xml.toxml())
        d.addCallback(lambda response: response[0]['http_code'] == 200)
        return d

    def add(self, index, *docs):
        xml = Document()
        root = xml.createElement('add')
        for doc in docs:
            doc_element = xml.createElement('doc')
            for key, value in doc.iteritems():
                field = xml.createElement('field')
                field.setAttribute("name", key)
                text = xml.createTextNode(value)
                field.appendChild(text)
                doc_element.appendChild(field)
            root.appendChild(doc_element)
        xml.appendChild(root)
        return self._do_xml_request("/solr/%s/update" % index, xml)

    index = add

    def delete(self, index, docs=None, queries=None):
        xml = Document()
        root = xml.createElement('delete')
        if docs:
            for doc in docs:
                doc_element = xml.createElement('id')
                text = xml.createTextNode(doc)
                doc_element.appendChild(text)
                root.appendChild(doc_element)
        if queries:
            for query in queries:
                query_element = xml.createElement('query')
                text = xml.createTextNode(query)
                query_element.appendChild(text)
                root.appendChild(query_element)

        xml.appendChild(root)
        return self._do_xml_request("/solr/%s/update" % index, xml)

    remove = delete

    def search(self, index, query, **params):
        options = {'q': query, 'wt': 'json'}
        options.update(params)

        def decode_results(response):
            headers, data = response
            decoder = self.get_decoder(headers['content-type'])
            return decoder(data)

        url = "/solr/%s/select" % index
        host, port, url = util.build_rest_path(self._client, prefix=url,
                                               params=options)
        d = util.http_request_deferred('GET', host, port, url)
        d.addCallback(decode_results)
        return d

    select = search
