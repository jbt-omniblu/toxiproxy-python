# coding: utf-8

from contextlib import contextmanager
from typing import Optional, Union
import threading
import time

from .api import APIConsumer
from .toxic import Toxic


class Proxy(object):
    """ Represents a Proxy object """

    def __init__(self, **kwargs):

        self.name = kwargs["name"]
        self.upstream = kwargs["upstream"]
        self.enabled = kwargs["enabled"]
        self.listen = kwargs["listen"]

    def get_listen_host(self):
        return self.listen.rsplit(':', 1)[0]

    def get_listen_port(self):
        return self.listen.rsplit(':', 1)[1]

    @contextmanager
    def down(self):
        """ Takes the proxy down while in the context """

        try:
            self.disable()
            yield self
        finally:
            self.enable()

    def toxics(self):
        """ Returns all toxics tied to the proxy """

        toxics = APIConsumer.get("/proxies/%s/toxics" % self.name).json()
        toxics_dict = {}

        for toxic in toxics:
            toxic_name = toxic["name"]
            toxic.update({'proxy': self.name})

            # Add the new toxic to the proxy toxics collection
            toxics_dict.update({toxic_name: Toxic(**toxic)})

        return toxics_dict

    def get_toxic(self, toxic_name):
        """ Retrive a toxic if it exists """

        toxics = self.toxics()
        if toxic_name in toxics:
            return toxics[toxic_name]
        else:
            return None

    def add_toxic(self, **kwargs):
        """ Add a toxic to the proxy """

        toxic_type = kwargs["type"]
        stream = kwargs["stream"] if "stream" in kwargs else "downstream"
        name = kwargs["name"] if "name" in kwargs else "%s_%s" % (toxic_type, stream)
        toxicity = kwargs["toxicity"] if "toxicity" in kwargs else 1.0
        attributes = kwargs["attributes"] if "attributes" in kwargs else {}

        # Lets build a dictionary to send the data to create the Toxic
        json = {
            "name": name,
            "type": toxic_type,
            "stream": stream,
            "toxicity": toxicity,
            "attributes": attributes
        }

        APIConsumer.post("/proxies/%s/toxics" % self.name, json=json).json()

    def edit_toxic(self, **kwargs):
        """ Edit an existing toxic """

        toxic_type = kwargs["type"]
        stream = kwargs["stream"] if "stream" in kwargs else "downstream"
        name = kwargs["name"] if "name" in kwargs else "%s_%s" % (toxic_type, stream)
        toxicity = kwargs["toxicity"] if "toxicity" in kwargs else 1.0
        attributes = kwargs["attributes"] if "attributes" in kwargs else {}

        # Lets build a dictionary to send the data to create the Toxic
        json = {
            "name": name,
            "type": toxic_type,
            "stream": stream,
            "toxicity": toxicity,
            "attributes": attributes
        }

        APIConsumer.post("/proxies/{}/toxics/{}".format(self.name, name), json=json).json()

    def destroy_toxic(self, toxic_name):
        """ Destroy the given toxic """

        delete_url = "/proxies/%s/toxics/%s" % (self.name, toxic_name)
        return bool(APIConsumer.delete(delete_url))

    def destroy(self):
        """ Destroy a Toxiproxy proxy """

        return bool(APIConsumer.delete("/proxies/%s" % self.name))

    def disable(self):
        """
        Disables a Toxiproxy - this will drop all active connections and
        stop the proxy from listening.
        """

        return self.__enable_proxy(False)

    def enable(self):
        """
        Enables a Toxiproxy - this will cause the proxy to start listening again.
        """

        return self.__enable_proxy(True)

    def __enable_proxy(self, enabled=False):
        """ Enables or Disable a proxy """

        # Lets build a dictionary to send the data to the Toxiproxy server
        json = {
            "enabled": enabled,
        }

        APIConsumer.post("/proxies/%s" % self.name, json=json).json()
        self.enabled = enabled

    # Latency toxic
    def add_latency_toxic(self, latency: int, jitter: int = 0,
                          stream: str = 'downstream', name: Optional[str] = None,
                          toxicity: Union[int, float] = 1.0) -> str:
        attributes = {'latency': latency, 'jitter': jitter}
        name = name or f'latency_{stream}'
        self.add_toxic(type='latency', stream=stream, name=name, toxicity=toxicity, attributes=attributes)
        return name

    # Bandwidth toxic
    def add_bandwidth_toxic(self, rate: int,
                            stream: str = 'downstream', name: Optional[str] = None,
                            toxicity: Union[int, float] = 1.0) -> str:
        attributes = {'rate': rate}
        name = name or f'bandwidth_{stream}'
        self.add_toxic(type='bandwidth', stream=stream, name=name, toxicity=toxicity, attributes=attributes)
        return name

    # Slicer toxic
    def add_slicer_toxic(self, average_size: int, size_variation: int = 0,
                         stream: str = 'downstream', name: Optional[str] = None,
                         toxicity: Union[int, float] = 1.0) -> str:
        attributes = {'average_size': average_size, 'size_variation': size_variation}
        name = name or f'slicer_{stream}'
        self.add_toxic(type='slicer', stream=stream, name=name, toxicity=toxicity, attributes=attributes)
        return name

    # Slow close toxic
    def add_slow_close_toxic(self, delay: int,
                             stream: str = 'downstream', name: Optional[str] = None,
                             toxicity: Union[int, float] = 1.0) -> str:
        attributes = {'delay': delay}
        name = name or f'slow_close_{stream}'
        self.add_toxic(type='slow_close', stream=stream, name=name, toxicity=toxicity, attributes=attributes)
        return name

    # Timeout toxic
    def add_timeout_toxic(self, timeout: int,
                          stream: str = 'downstream', name: Optional[str] = None,
                          toxicity: Union[int, float] = 1.0) -> str:
        attributes = {'timeout': timeout}
        name = name or f'timeout_{stream}'
        self.add_toxic(type='timeout', stream=stream, name=name, toxicity=toxicity, attributes=attributes)
        return name

    # Limit data toxic
    def add_limit_data_toxic(self, bytes: int,
                             stream: str = 'downstream', name: Optional[str] = None,
                             toxicity: Union[int, float] = 1.0) -> str:
        attributes = {'bytes': bytes}
        name = name or f'limit_data_{stream}'
        self.add_toxic(type='limit_data', stream=stream, name=name, toxicity=toxicity, attributes=attributes)
        return name

    # Destroy toxic by name after waiting for specified duration
    def destroy_toxic_after_delay(self, name: str, seconds: float) -> threading.Thread:
        thread = threading.Thread(target=lambda: time.sleep(seconds) or self.destroy_toxic(name))
        thread.start()
        return thread
